package com.tianchi.django.common.loader;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;

import com.google.common.base.Charsets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
public abstract class AbstractDataLoader<T> {

    public abstract T load();//子类实现数据加载

    /**
     * 读取工程目录下 "dir/subdir/fileName"文件信息并转化为string类型
     */
    protected String readFileToString(@NonNull String dir, @NonNull String subdir, @NonNull String fileName) {

        try {

            log.info("read file from directory:{} , sub dir:{} , fileName:{}", dir, subdir, fileName);

            File file = Paths.get(dir, subdir, fileName).toFile();

            return FileUtils.readFileToString(file, Charsets.UTF_8);

        } catch (IOException e) {

            return "";

        }

    }

    /**
     * 读取resouces下"directory/fileName"文件信息并转化为string类型
     */
    protected String readFileToString(@NonNull String directory, @NonNull String fileName) {

        try {

            String path = Paths.get(directory, fileName).toString();

            log.info("read file from directory:{} , fileName:{}", directory, fileName);

            URI uri = Objects.requireNonNull(AbstractDataLoader.class.getClassLoader().getResource(path)).toURI();

            return FileUtils.readFileToString(Paths.get(uri).toFile(), Charsets.UTF_8);

        } catch (IOException | URISyntaxException e) {

            log.error(e.getMessage(), e);

            throw new RuntimeException(e);
        }
    }

}
