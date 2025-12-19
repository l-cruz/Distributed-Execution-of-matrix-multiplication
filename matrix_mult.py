from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMatrixMultiply(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_by_k,
                   reducer=self.reducer_join),
            MRStep(mapper=self.mapper_ij,
                   reducer=self.reducer_sum)
        ]

    # JOB 1
    def mapper_by_k(self, _, line):
        tag, a, b, v = line.split(",")
        a, b, v = int(a), int(b), float(v)

        if tag == "A":
            yield b, ("A", a, v)   # k -> (A, i, val)
        else:
            yield a, ("B", b, v)   # k -> (B, j, val)

    def reducer_join(self, k, values):
        A = []
        B = []

        for tag, idx, v in values:
            if tag == "A":
                A.append((idx, v))
            else:
                B.append((idx, v))

        for i, aik in A:
            for j, bkj in B:
                yield (i, j), aik * bkj

    # JOB 2
    def mapper_ij(self, ij, value):
        i, j = ij
        yield f"{i},{j}", value

    def reducer_sum(self, ij, values):
        yield ij, sum(values)

if __name__ == "__main__":
    MRMatrixMultiply.run()
