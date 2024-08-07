"""Convenience functions for paths."""

import os


def get_data_store():
    """Set data location: in the repo directory"""
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(cur_dir)
    data_dir = os.path.join(base_dir, "data")
    return data_dir


def top_folder(rel=""):
    """Returns a path relative to the top-level repo folder. This will work regardless
    of where the function is imported or called from.
    """
    return os.path.join(
        os.path.abspath(os.path.join(os.path.realpath(__file__), "../")), rel
    )

def data_folder(rel=""):
    """Returns a path relative to the `data` folder."""
    return os.path.join(get_data_store(), rel).replace("\\", "/")


def downloads_folder(rel=""):
    return os.path.join(data_folder("downloads"), rel).replace("\\", "/")


def outputs_folder(rel=""):
    return os.path.join(data_folder("outputs"), rel).replace("\\", "/")


def results_folder(rel=""):
    return os.path.join(data_folder("results"), rel).replace("\\", "/")


def containing_folder(filepath: str) -> str:
    """Returns the folder containing `filepath`."""
    return os.path.dirname(os.path.realpath(filepath))


def make_containing_folder(filepath: str):
    """Make sure the the folder where `filepath` goes exists."""
    os.makedirs(containing_folder(filepath), exist_ok=True)

def reference_table_folder(rel=""):
    return os.path.join(top_folder("reference_tables"), rel).replace("\\", "/")