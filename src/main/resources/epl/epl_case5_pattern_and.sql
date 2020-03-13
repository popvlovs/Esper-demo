
A B C C B A

A, START A WATCHER(B, C)
            |-- A.attr1 = C.attr2 and B.attr2 = C.attr2
            |-- B comes
            |   |-- CriteriaGraph.check(A, B)
            |       |-- False: destroy current branch
            |       `-- True: store B
            |           `-- C comes
            |               `-- CriteriaGraph.check(A, B, C)
            `-- C comes
                |-- CriteriaGraph.check(A, C)
                `-- B comes
                    `-- CriteriaGraph.check(A, C, B)

B, START A WATCHER(A, C)