
__all__ = [
    
]


try:
    # provided by dagster-polars[delta]
    from dagster_polars.jobs.delta import optimize_deltalake_table


    __all__.extend(["optimize_deltalake_table"])
except ImportError:
    pass
