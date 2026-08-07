interface BoardQueue {
  tail: Promise<void>;
  users: number;
}

/**
 * Orders database mutation through local publication/eviction for one board.
 * PostgreSQL advisory locks remain the durable sequencing authority.
 */
export class BoardDeliveryCoordinator {
  private readonly queues = new Map<string, BoardQueue>();

  async run<T>(boardId: string, task: () => Promise<T>): Promise<T> {
    let queue = this.queues.get(boardId);
    if (!queue) {
      queue = { tail: Promise.resolve(), users: 0 };
      this.queues.set(boardId, queue);
    }
    const previous = queue.tail;
    let release!: () => void;
    queue.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    queue.users += 1;
    await previous;
    try {
      return await task();
    } finally {
      release();
      queue.users -= 1;
      if (queue.users === 0 && this.queues.get(boardId) === queue) this.queues.delete(boardId);
    }
  }

  get activeBoardCount(): number {
    return this.queues.size;
  }
}
