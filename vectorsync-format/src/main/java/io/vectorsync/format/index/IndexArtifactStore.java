package io.vectorsync.format.index;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Moves index artifact files between a local working directory and durable storage.
 *
 * <p>Goes through Iceberg's {@link FileIO} rather than an S3 client directly, so the same code
 * path serves an {@code s3a://} warehouse in production and a local filesystem in tests, and the
 * artifact inherits whatever credentials the catalog was configured with.
 *
 * <p>Artifacts are opaque binaries and so live beside the warehouse rather than inside a table.
 * Only the {@link IndexManifestEntry} pointing at them is a table, which is what keeps the
 * metadata queryable from any engine while the index format stays an implementation detail.
 */
@Slf4j
public final class IndexArtifactStore {

    private static final int BUFFER_SIZE = 1 << 16;

    private final FileIO fileIO;

    public IndexArtifactStore(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    /**
     * Copies every regular file in {@code localDir} to {@code indexUri}.
     *
     * @return the uploaded file names, sorted for deterministic manifest contents
     */
    public List<String> upload(Path localDir, String indexUri) {
        List<String> uploaded = new ArrayList<>();

        try (Stream<Path> files = Files.list(localDir)) {
            List<Path> sorted = files.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();

            for (Path file : sorted) {
                String name = file.getFileName().toString();
                OutputFile target = fileIO.newOutputFile(join(indexUri, name));

                try (InputStream in = Files.newInputStream(file);
                     PositionOutputStream out = target.createOrOverwrite()) {
                    in.transferTo(out);
                }

                uploaded.add(name);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to upload index artifact to " + indexUri, e);
        }

        log.info("Uploaded {} index artifact files to {}", uploaded.size(), indexUri);
        return uploaded;
    }

    /** Materializes a stored artifact into {@code localDir} so a local reader can open it. */
    public void download(String indexUri, List<String> fileNames, Path localDir) {
        try {
            Files.createDirectories(localDir);

            for (String name : fileNames) {
                InputFile source = fileIO.newInputFile(join(indexUri, name));

                try (SeekableInputStream in = source.newStream();
                     OutputStream out = Files.newOutputStream(localDir.resolve(name))) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        out.write(buffer, 0, read);
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to download index artifact from " + indexUri, e);
        }

        log.info("Downloaded {} index artifact files from {}", fileNames.size(), indexUri);
    }

    /** Best-effort cleanup of a superseded artifact. */
    public void delete(String indexUri, List<String> fileNames) {
        for (String name : fileNames) {
            try {
                fileIO.deleteFile(join(indexUri, name));
            } catch (Exception e) {
                log.warn("Failed to delete index artifact file {}/{}: {}", indexUri, name, e.getMessage());
            }
        }
    }

    private static String join(String prefix, String name) {
        return prefix.endsWith("/") ? prefix + name : prefix + "/" + name;
    }
}
