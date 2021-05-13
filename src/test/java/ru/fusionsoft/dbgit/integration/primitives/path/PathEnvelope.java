package ru.fusionsoft.dbgit.integration.primitives.path;

import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Iterator;

public abstract class PathEnvelope implements Path {

    private final SafeScalar<Path> origin;

    public PathEnvelope(Scalar<Path> origin) {
        this.origin = new SafeScalarOf<>(
            new StickyScalar<>(
                origin
            )
        );
    }

    @Override
    public String toString() {
        return this.origin.value().toString();
    }

    @Override
    public final FileSystem getFileSystem() {
        return this.origin.value().getFileSystem();
    }

    @Override
    public final boolean isAbsolute() {
        return this.origin.value().isAbsolute();
    }

    @Override
    public final Path getRoot() {
        return this.origin.value().getRoot();
    }

    @Override
    public final Path getFileName() {
        return this.origin.value().getFileName();
    }

    @Override
    public final Path getParent() {
        return this.origin.value().getParent();
    }

    @Override
    public final int getNameCount() {
        return this.origin.value().getNameCount();
    }

    @Override
    public final Path getName(int i) {
        return this.origin.value().getName(i);
    }

    @Override
    public final Path subpath(int i, int i1) {
        return this.origin.value().subpath(i, i1);
    }

    @Override
    public final boolean startsWith(Path path) {
        return this.origin.value().startsWith(path);
    }

    @Override
    public final boolean endsWith(Path path) {
        return this.origin.value().endsWith(path);
    }

    @Override
    public final Path normalize() {
        return this.origin.value().normalize();
    }

    @Override
    public final Path resolve(Path path) {
        return this.origin.value().resolve(path);
    }

    @Override
    public final Path relativize(Path path) {
        return this.origin.value().relativize(path);
    }

    @Override
    public final URI toUri() {
        return this.origin.value().toUri();
    }

    @Override
    public final Path toAbsolutePath() {
        return this.origin.value().toAbsolutePath();
    }

    @Override
    public final Path toRealPath(LinkOption... linkOptions) throws IOException {
        return this.origin.value().toRealPath(linkOptions);
    }

    @Override
    public final WatchKey register(WatchService watchService, WatchEvent.Kind<?>[] kinds, WatchEvent.Modifier... modifiers) throws IOException {
        return this.origin.value().register(watchService,kinds,modifiers);
    }

    @Override
    public final int compareTo(Path path) {
        return this.origin.value().compareTo(path);
    }

    @Override
    public final Iterator<Path> iterator() {
        return origin.value().iterator();
    }

    @Override
    public final WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        return this.origin.value().register(watcher, events);
    }

    @Override
    public final boolean startsWith(String other) {
        return this.origin.value().startsWith(other);
    }

    @Override
    public final boolean endsWith(String other) {
        return this.origin.value().endsWith(other);
    }

    @Override
    public final Path resolve(String other) {
        return this.origin.value().resolve(other);
    }

    @Override
    public final Path resolveSibling(Path other) {
        return this.origin.value().resolveSibling(other);
    }

    @Override
    public final Path resolveSibling(String other) {
        return this.origin.value().resolveSibling(other);
    }

    @Override
    public final File toFile() {
        return this.origin.value().toFile();
    }
}
