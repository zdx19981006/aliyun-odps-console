package com.aliyun.openservices.odps.console.utils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class FileStorage<T> {

  private final ReentrantLock lock = new ReentrantLock();
  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final String filePath;
  private final Class<T> clazz;

  public FileStorage(String filePath, Class<T> clazz) {
    this.filePath = filePath;
    this.clazz = clazz;
  }

  public void save(T data) throws IOException {
    lock.lock();
    try {
      File file = new File(filePath);
      // 创建父目录（如果不存在）
      Files.createDirectories(Paths.get(file.getParent()));
      // 如果文件不存在，则创建新文件
      if (!file.exists()) {
        boolean created = file.createNewFile();
        if (!created) {
          throw new IOException("Failed to create new file: " + filePath);
        }
      }
      try (FileWriter writer = new FileWriter(filePath)) {
        gson.toJson(data, writer);
      }
    } finally {
      lock.unlock();
    }
  }

  public T load() throws IOException {
    lock.lock();
    try {
      File file = new File(filePath);
      if (!file.exists()) {
        return null;
      }
      try (FileReader reader = new FileReader(file)) {
        return gson.fromJson(reader, (Type) clazz);
      }
    } finally {
      lock.unlock();
    }
  }
}
