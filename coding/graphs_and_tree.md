# Graphs
- A graph is a data structure composed of a collection of nodes(vertices) and edges(arrows, pointers).
- Graphs are a non-linear data structure (as opposed to a linked list, stack, queue).
- A graph might consist of multiple isolated subgraphs. If, there is a path between every pair of vertices it is called a "connected graph".
- There are two ways to represent graph:
  - List
  - Matrices
    - quando for um grafo pequeno, poucos edges, é a melhor escolha
    - acesso direto na memória
    - BAD: espaço em memória
- Common Operations in Graphs: traversal
  - Depth first search
  - Beadth first search

### Space and Time Complexity
```
Space -> O(V + E)
Time  -> O(V + E)
```

### Total Edges of Graph
<img src="https://latex.codecogs.com/svg.image?\frac{n(n&space;-1)}{2}" title="\frac{n(n -1)}{2}" />
n = nodes


### Types of graphs
- Directed graphs (DAG)

![image](https://user-images.githubusercontent.com/12896018/153716436-515af293-cc6a-4999-b543-63cbd1a308ea.png)

<br/>

- Undirected graphs
 
![image](https://user-images.githubusercontent.com/12896018/153716427-bb4a88e8-1711-49c1-9d48-404e9b5a9160.png)


## Graph Traversal
```
queue = []
list_visited_vertices = []
list_current = None
```

### Tradeoffs Breadth-first Search X Depth-first Search
- DFS is a bit simpler to implement because use recursion
- BFS can useful to find the shortest path, whereas DFS may traverse one node very deeply.

---

# Binary Search Tree

### Most Important Questions
- Validation
- Traversal
- Find Value






---
