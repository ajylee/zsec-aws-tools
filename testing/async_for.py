from zsec_aws_tools.async_tools import asyncify, gather_and_run, asyncify2
import time
import asyncio
from toolz.curried import do
from toolz import pipe



class AsyncIteratorWrapper:
    def __init__(self, obj):
        self._it = iter(obj)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            value = next(self._it)
        except StopIteration:
            raise StopAsyncIteration
        return value


class AsyncIteratorWrapper2:
    def __init__(self, thunks):
        self._it = iter(thunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            value = await next(self._it)
        except StopIteration:
            raise StopAsyncIteration
        return value


async def thunk1():
    print('thunk 1')
    async for x in AsyncIteratorWrapper(range(3)):
        time.sleep(1)
        print(x)

    #for x in AsyncIteratorWrapper(range(3)):
    #    time.sleep(1)
    #    print(x)

def f(x): 
    time.sleep(1)
    print(x)
    return x

af = asyncify2(f)

async def thunk2():
    print('thunk 2')
    #async for x in AsyncIteratorWrapper(range(3)):
    await asyncio.gather(*map(af, range(3)))


async def thunk2b():
    print('thunk 2b')
    #async for x in AsyncIteratorWrapper(range(3)):
    await asyncio.gather(*(asyncify(f, ii) for ii in range(3)))


async def thunk2c():
    print('thunk 2c')
    results = []
    for x in map(af, range(3)):
    #for x in range(3):
        #res = await x
        await x
        #print('res:', res) 
        #results.append(res)
    for res in results:
        yield res


async def thunk3():
    print('thunk 3')
    #async for x in AsyncIteratorWrapper(range(3)):
    #async for x in thunk2c():
    async for x in thunk2c():
        print('x', x)
        #yield x


async def thunk4():
    print('thunk 3')
    #async for x in AsyncIteratorWrapper(range(3)):
    return await asyncio.gather(thunk3(), thunk3())

#gather_and_run([thunk1()])
#asyncio.run(thunk2())
asyncio.run(thunk4())
#gather_and_run(map(f, range(3)))

# broken
#asyncio.run(asyncio.gather(thunk))
