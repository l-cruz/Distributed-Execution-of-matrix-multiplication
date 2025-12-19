import numpy as np

def write_matrix(path, tag, M):
    with open(path, "w") as f:
        n = M.shape[0]
        for i in range(n):
            for j in range(n):
                if M[i, j] != 0.0:
                    f.write(f"{tag},{i},{j},{M[i,j]}\n")

if __name__ == "__main__":
    N = 200  # tamaño de la matriz
    np.random.seed(0)

    A = np.random.rand(N, N)
    B = np.random.rand(N, N)

    write_matrix("A.txt", "A", A)
    write_matrix("B.txt", "B", B)

    print("Archivos A.txt y B.txt generados")
