package com.vpon.ssp.report.archive.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class PoolWithBlockingQueue <T>
{
    public interface ObjectPoolFactory<T> {
        T create();
    }

    private final BlockingQueue<T> objects;
    private ObjectPoolFactory<T> factory = null;

    public PoolWithBlockingQueue(ObjectPoolFactory<T> objFactory, int capacity) {
        this.factory = objFactory;
        this.objects = new ArrayBlockingQueue<>(capacity);
        for (long i = 0; i < capacity; i++) {
            objects.add(factory.create());
        }
    }

    public T borrowObject() {
        T t = this.objects.poll();
        if (t == null) {
            t = (factory != null) ? factory.create() : null;
        }
        return t;
    }

    public void returnObject(T object) {
        this.objects.offer(object);
    }
}
