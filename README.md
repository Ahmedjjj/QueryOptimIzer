# QueryOptimizer

Modifications made:

1. Block Nested Join
This join algorithm was implemented in the class BlockNestedJoin.java. The makeExecPlan method of the RandomOptimizer.java was modified to support this join type. The join cost calculation is added in the getStatistics method in PlanCost.java.  
2. Index Nested Join
This join algorithm was implemented in the class IndexNestedJoin.java. The makeExecPlan method of the RandomOptimizer.java was modified to support this join type. The join cost calculation is added in the getStatistics method in PlanCost.java.
3. External Sort
We implemented a general version of external sort, that can sort on any number of attributes. This is easily done by using
Java's Comparator interface. The comparison is done from left to right, i.e we compare the first attribute then the second if the 
first is comparison is equal, etc.. The sorting can also do duplicate elimination. This is done as efficiently as possible:
We remove duplicates from each sorted run as they are generated. Finally we merge the sorted runs while making sure that 
no duplicate is taken (Comparator is also used here).
3. Distinct
We implemented distinct which was done by sorting the file and removing duplicates.The attributes that we sort on
are the attributes of the schema of the underlying Operator.
4. OrderBy
Using the External sort implemented previously, orderBy is straightforward to implement. We just set the sorting attributes
to the right hand side of the ORDERBY operation.
5. Random Optimizer
We implemented the Two Phase Optimization from the provided paper “Randomized Algorithms for Optimizing Large Join Queries”. Simulated Annealing was implemented on top of the already existing Iterative Improvement technique used initially in the optimizer. The optimization was added in the method simulatedAnnealing in RandomOptimizer.java. This method takes the initial result provided by the Iterative Improvement implemented in getOptimizedPlan and runs the simulated annealing algorithm on it.  
6. Bugs identified:
The join cost calculation for Nested Join in the getStatistics method of PlanCost.java. Initially the cost was calculated as joincost = leftpages*rightpages, the cost was corrected to be joincost = leftpages + lefttuples * rightpages. 



