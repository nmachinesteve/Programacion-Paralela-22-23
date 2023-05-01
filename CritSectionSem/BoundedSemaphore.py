from multiprocessing import Process
from multiprocessing import current_process
from multiprocessing import Value, Array
from multiprocessing import Process, BoundedSemaphore

N = 8

def task (s, common, tid):
    a = 0
    for i in range(100):
        print(f'{tid}-{i}: Non-critical Section')
        a = a +1
        print(f'{tid}-{i}: End of non-critical Section')
        s.acquire()
        try:
            print(f'{tid}-{i}: Critical Section')
            v = common.value + 1
            print(f'{tid}-{i}: Inside Critical Section')
            common.value = v
            print(f'{tid}-{i}: End of Critical Section')
        finally:
            s.release()

def main():
    lp = []
    common = Value('i',0)
    s = BoundedSemaphore(1)
    for tid in range(N):
        lp.append(Process(target=task, args=(s,common,tid)))
    print(f"Valor inicial del contador {common.value}")
    for p in lp:
        p.start()
    for p in lp:
        p.join()
    print(f"Valor final del contador {common.value}")
    print("Fin")

if __name__ == "__main__":
    main()