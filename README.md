# QueryOptimizer

Modifications made:

1. Block Nested Join
This join algorithm was implemented in the class BlockNestedJoin.java. The makeExecPlan method of the RandomOptimizer.java was modified to support this join type. The join cost calculation is added in the getStatistics method in PlanCost.java.  
2. Index Nested Join
3. Distinct
4. OrderBy
5. Random Optimizer
We implemented the Two Phase Optimization from the provided paper “Randomized Algorithms for Optimizing Large Join Queries”. Simulated Annealing was implemented on top of the already existing Iterative Improvement technique used initially in the optimizer. The optimization was added in the method simulatedAnnealing in RandomOptimizer.java. This method takes the initial result provided by the Iterative Improvement implemented in getOptimizedPlan and runs the simulated annealing algorithm on it.  
6. Bugs identified:
The join cost calculation for Nested Join in the getStatistics method of PlanCost.java. Initially the cost was calculated as joincost = leftpages*rightpages, the cost was corrected to be joincost = leftpages + lefttuples * rightpages. 



