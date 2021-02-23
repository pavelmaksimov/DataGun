# -*- coding: utf-8 -*-

from datagun import DataSet


def test_transpont():
    data = [[1, 2, 3], [10, 20, 30], [100, 200, 300], [1000, 2000, 3000]]
    dataset1 = DataSet(data, orient="columns")

    assert dataset1.T().to_dict() == [
        {"0": 1, "1": 2, "2": 3},
        {"0": 10, "1": 20, "2": 30},
        {"0": 100, "1": 200, "2": 300},
        {"0": 1000, "1": 2000, "2": 3000},
    ]

    dataset2 = DataSet(data, orient="values")
    assert dataset2.T().to_dict() == [
        {"0": 1, "1": 10, "2": 100, "3": 1000},
        {"0": 2, "1": 20, "2": 200, "3": 2000},
        {"0": 3, "1": 30, "2": 300, "3": 3000},
    ]
