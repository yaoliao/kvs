use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::{fs, io};

use crossbeam_skiplist::SkipMap;
use failure::_core::cell::RefCell;
use failure::_core::sync::atomic::AtomicU64;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::engines::KvsEngine;
use crate::{KvsError, Result};
use std::collections::btree_map::Entry;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
#[derive(Clone)]
pub struct KvStore {
    /// 日志路径
    path: Arc<PathBuf>,

    /// 索引
    index: Arc<SkipMap<String, CommandPos>>,

    /// 读取
    reader: KvStoreReader,

    /// 写入
    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {
    /// open KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir = path.into();
        fs::create_dir_all(&dir)?;

        let mut readers: BTreeMap<u64, BufReaderWithPos<File>> = BTreeMap::new();
        let index: Arc<SkipMap<String, CommandPos>> = Arc::new(SkipMap::new());

        let mut uncompressed = 0;

        let sort_gen = sorted_gen_list(&dir)?;

        for &gen in &sort_gen {
            let gen_path = log_path(&dir, gen);
            let mut br = BufReaderWithPos::new(File::open(gen_path)?)?;
            uncompressed += load(&index, &mut br, gen)?;
            readers.insert(gen, br);
        }

        let last_gen = match sort_gen.last() {
            Some(&gen) => gen,
            None => {
                let file = creat_file(&log_path(&dir, 0_u64))?;
                let br = BufReaderWithPos::new(file)?;
                readers.insert(0_u64, br);
                0_u64
            }
        };

        let path = log_path(&dir, last_gen);

        let mut file = creat_file(&path)?;
        let len = file.seek(SeekFrom::End(0))?;
        let mut writer = BufWriterWithPos::new(file)?;
        writer.seek(SeekFrom::Start(len))?;

        let path = Arc::new(dir);
        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point: Arc::new(AtomicU64::new(0)),
            readers: RefCell::new(readers),
        };

        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            current_gen: last_gen,
            uncompressed,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            index,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

struct KvStoreReader {
    path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    current_gen: u64,

    uncompressed: u64,

    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandPos>>,
}

impl KvStoreReader {
    /// 移除已经被压缩过的 reader
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let gen = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= gen {
                return;
            }
            readers.remove(&gen);
        }
    }

    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();

        if let Entry::Vacant(e) = readers.entry(cmd_pos.gen) {
            let gen_path = log_path(&self.path, cmd_pos.gen);
            let reader = BufReaderWithPos::new(File::open(gen_path)?)?;
            e.insert(reader);
        }

        let reader = readers.get_mut(&cmd_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let take = reader.take(cmd_pos.size);
        f(take)
    }

    /// 根据 CommandPos 从 kvs 中读取 Command
    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}


impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key.clone(), value);

        let pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        if let Some(old_cmd) = self.index.get(&key) {
            self.uncompressed += old_cmd.value().size;
        }
        self.index.insert(
            key,
            CommandPos {
                gen: self.current_gen,
                pos,
                size: self.writer.pos - pos,
            },
        );

        if self.uncompressed > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key.clone());
            let pos = self.writer.pos;

            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Some(cmd) = self.index.remove(&key) {
                self.uncompressed += cmd.value().size;
                // remove 命令自己的长度
                self.uncompressed += self.writer.pos - pos;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        // 压缩日志的 gen
        let compact_gen = self.current_gen + 1;
        // 新日志的 gen
        self.current_gen += 2;
        self.writer = BufWriterWithPos::new(creat_file(&log_path(&self.path, self.current_gen))?)?;

        let file = creat_file(&log_path(&self.path, compact_gen))?;
        let mut buffer_writer = BufWriterWithPos::new(file)?;

        let mut new_pos = 0u64;
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut buffer_writer)?)
            })?;
            self.index.insert(
                entry.key().clone(),
                CommandPos {
                    gen: entry.value().gen,
                    pos: new_pos,
                    size: len,
                },
            );
            new_pos += len;
        }
        buffer_writer.flush()?;

        self.reader.safe_point.store(compact_gen, Ordering::SeqCst);
        self.reader.close_stale_handles();

        let old_gen = sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compact_gen);

        for gen in old_gen {
            fs::remove_file(log_path(&self.path, gen))?;
        }
        self.uncompressed = 0;
        Ok(())
    }
}

fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn creat_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .append(true)
        .open(&path)?;
    Ok(file)
}

/// 加载日志到索引文件
fn load(
    index: &SkipMap<String, CommandPos>,
    reader: &mut BufReaderWithPos<File>,
    gen: u64,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

    let mut uncompacted: u64 = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, value: _ } => {
                if let Some(cmd) = index.get(&key) {
                    uncompacted += cmd.value().size;
                }
                index.insert(
                    key,
                    CommandPos {
                        gen,
                        pos,
                        size: new_pos - pos,
                    },
                );
            }
            Command::Remove { key } => {
                if let Some(_entry) = index.remove(&key) {
                    uncompacted += new_pos - pos;
                }
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

/// 获取日志目录
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandPos {
    gen: u64,
    pos: u64,
    size: u64,
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<BufReaderWithPos<R>> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = self.reader.seek(pos)?;
        self.pos = pos;
        Ok(pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<R: Write + Seek> {
    writer: BufWriter<R>,
    pos: u64,
}

impl<R: Write + Seek> BufWriterWithPos<R> {
    fn new(mut inner: R) -> Result<BufWriterWithPos<R>> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<R: Write + Seek> Write for BufWriterWithPos<R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<R: Write + Seek> Seek for BufWriterWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
