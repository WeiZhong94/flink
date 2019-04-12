import time

def fun():
    for i in range(20):
        yield i
        print('execute after first yield')
        yield i

if __name__ == '__main__':
    a = fun()
    for y in a:
        time.sleep(1)
        print("next() has been executed and value is " + str(y) + ", if yield x == collect(x), there will be some output before this message")

