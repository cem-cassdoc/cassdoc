package cwdrg.util.async.iterator;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import org.slf4j.LoggerFactory;

import cwdrg.lg.Lg;

public class BlockingIterator<T> implements Iterator<T> {
	private static transient final Lg log = new Lg(
			LoggerFactory.getLogger(BlockingIterator.class));

	private BlockingQueue<T> queue;
	// private BlockingQueue<T> queue = new ArrayBlockingQueue<T>(10000);
	private T sentinel = (T) new Object();
	private T next;

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;

		}
		try {
			next = queue.take(); // blocks if necessary
		} catch (InterruptedException ie) {
			throw Lg.err(new RuntimeException("AsyncIterator: TAKE", ie));
		}
		if (next == sentinel) {
			log.dbg(log.d() ? "sentinel encountered, ending iterator" : "",
					null);
			return false;
		}
		return true;
	}

	@Override
	public T next() {
		T tmp = next;
		next = null;
		return tmp;
	}

	public T getSentinel() {
		return sentinel;
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<T> queue) {
		this.queue = queue;
	}

}
