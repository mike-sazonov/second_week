# Задача - Реализация очереди задач на основе базы данных
# Описание:
#
# Ваша задача — реализовать очередь задач, которая хранится в базе данных.
# Система должна позволять различным рабочим процессам (воркерами) безопасно
# брать задачи из очереди и обрабатывать их, гарантируя, что каждая задача
# будет взята и выполнена только одним процессом. Для этого необходимо
# использовать механизм транзакций с SELECT FOR UPDATE, чтобы избежать ситуаций,
# когда несколько процессов одновременно пытаются обработать одну и ту же задачу.
# Самостоятельно ознакомьтесь с командой SKIP LOCKED которая может использоваться вместе с FOR UPDATE.


import asyncio
import asyncpg
import datetime


async def fetch_task(pool, worker_id):
    # находим задачу, ставим статус processing и назначаем воркеру
    await pool.fetch('''
        WITH Next_tasks AS (
        SELECT *
        FROM Tasks
        WHERE STATUS = 'pending'
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        )
        UPDATE Tasks
        SET status = 'processing',
            worker_id = $1
        FROM Next_tasks
        WHERE Tasks.id = Next_tasks.id
    ''', worker_id)


async def worker(pool, worker_id):
    # имитируем выполнение задачи, после присваиваем задаче статус completed и время выполнения
    await fetch_task(pool, worker_id)   # здесь блокируем задачу для других воркеров и назначаем текущему
    await asyncio.sleep(0.5)
    await pool.fetch('''
    UPDATE Tasks
    SET status = 'completed',
        updated_at = $1
    WHERE worker_id = $2
    ''', datetime.datetime.now(), worker_id)


async def main():
    pool = await asyncpg.create_pool('postgresql://postgres:1111@localhost/test')

    async with pool.acquire() as conn:
        await conn.fetch('''
            CREATE TABLE Tasks(
                id serial PRIMARY KEY,
                task_name VARCHAR(40),
                status VARCHAR(40) DEFAULT 'pending',
                worker_id INT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
        ''')

        # создаём очередь задач
        for i in range(5):
            await conn.fetch('''
                INSERT INTO Tasks(task_name, created_at) VALUES($1, $2);
            ''', f"task N {i}", datetime.datetime.now())

        # создаём воркеров
        await asyncio.gather(*[worker(pool, worker_id) for worker_id in range(5)])

    await pool.close()

    # await conn.execute('''
    #     DROP TABLE Tasks
    # ''')


asyncio.run(main())