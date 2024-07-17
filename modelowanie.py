import random
import time

random.seed(42)

#========================
N = 1000

A = [[random.uniform(-1.0, 1.0) for j in range(N)] for i in range(N)]

L = [[0.0 for j in range(N)] for i in range(N)]
U = [[0.0 for j in range(N)] for i in range(N)]

start = time.time()
for k in range(N):
    U[k][k] = A[k][k]
    for i in range(k + 1, N):
        L[i][k] = A[i][k] / U[k][k]
        U[k][i] = A[k][i]
    for i in range(k + 1, N):
        for j in range(k + 1, N):
            A[i][j] = A[i][j] - L[i][k] * U[k][j]
end = time.time()

elapsed_time = end - start
print(f"Elapsed time for 1000 matrix size: {elapsed_time:.2f} seconds")

#========================
N = 2000

A = [[random.uniform(-1.0, 1.0) for j in range(N)] for i in range(N)]

L = [[0.0 for j in range(N)] for i in range(N)]
U = [[0.0 for j in range(N)] for i in range(N)]

start = time.time()
for k in range(N):
    U[k][k] = A[k][k]
    for i in range(k + 1, N):
        L[i][k] = A[i][k] / U[k][k]
        U[k][i] = A[k][i]
    for i in range(k + 1, N):
        for j in range(k + 1, N):
            A[i][j] = A[i][j] - L[i][k] * U[k][j]
end = time.time()

elapsed_time = end - start
print(f"Elapsed time for 2000 matrix size: {elapsed_time:.2f} seconds")

#========================
N = 3000

A = [[random.uniform(-1.0, 1.0) for j in range(N)] for i in range(N)]

L = [[0.0 for j in range(N)] for i in range(N)]
U = [[0.0 for j in range(N)] for i in range(N)]

start = time.time()
for k in range(N):
    U[k][k] = A[k][k]
    for i in range(k + 1, N):
        L[i][k] = A[i][k] / U[k][k]
        U[k][i] = A[k][i]
    for i in range(k + 1, N):
        for j in range(k + 1, N):
            A[i][j] = A[i][j] - L[i][k] * U[k][j]
end = time.time()

elapsed_time = end - start
print(f"Elapsed time for 3000 matrix size: {elapsed_time:.2f} seconds")