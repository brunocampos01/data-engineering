"""
### Module - Orchestration for QA Library

#### Class:
    * NotebookData: running Databricks notebooks in parallel

#### Functions:
    * parallelNotebooks: Run multiple notebooks in parallel
"""
import IPython

class NotebookData:
    """
    ### This class is responsible for running Databricks notebooks in parallel

    #### Functions:
        * 
    """
    def __init__(self, path: str, timeout: int, parameters: dict = None, retry: int = 3):
        """
        ### Init Class

        #### Args:
            * path (str): path for running notebook
            * timeout (int): timeout time
            * parameters (dict, optional): parameters to pass to running notebook. Defaults to None.
            * retry (int, optional): number of retries. Defaults to 3.
        """
        self.path = path
        self.timeout = timeout
        self.parameters = parameters
        self.retry = retry

    def submit_notebook(notebook) -> str:
        """
        ### Submit execution of databricks notebook

        #### Args:
            * notebook (str): Name of the notebook to be run

        #### Returns:
            * str: Exits value from notebook
        """
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        try:
            if notebook.parameters:
                return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
            else:
                return dbutils.notebook.run(notebook.path, notebook.timeout)
        except Exception:
            print("{}.{}".format(notebook.parameters["dbName"], notebook.parameters["tbName"]))
            resultado.append("{},{}".format(notebook.parameters["dbName"], notebook.parameters["tbName"]))


def parallelNotebooks(notebooks, numInParallel):
    """
    ### Run multiple notebooks in parallel

    #### Args:
        * notebooks (list): List of notebooks names to be run
        * numInParallel (int): Number of notebooks to be run in parallel
    """
    from multiprocessing.pool import ThreadPool

    POOL = ThreadPool(numInParallel)
    return POOL.map(lambda x: x.submit_notebook(), notebooks)
