#!/usr/bin/env python
#-*- coding:utf-8 -*-
################################################################################
#
#
################################################################################
"""

Authors: xiaojian.jia@163.com 
Date:    2015/12/28
"""
import math
import string
import sys
import os


def wilson_confidence(positive, negative, z=1.96):
    """use lower bound of Wilson score confidence interval as the rate
    https://www.ltcconline.net/greenl/courses/201/estimation/smallConfLevelTable.htm
    Args: 
        positive [in]: positive value
        negative [in]: negative value
        z=2.58     -> 99%
        z=2.33     -> 98%
        z=1.96     -> 95%
        z=1.645    -> 90%
        z=1.44     -> 85%
        z=1.28     -> 80%
        z=1.15     -> 75%
        z=1.036    -> 70%
        z=0.841    -> 60%
        z=0.67449  -> 50%
    Returns:
        lower bound of Wilson score confidence interval
    """
    positive = max(0, positive)
    negative = max(0, negative)
    n = float(positive + negative)
    if n <= 0 or positive < 0 or negative < 0:
        return 0.0, 0.0
    p = float(positive) / n
    lower_score = (2 * n * p + z * z - z * math.sqrt(4 * n * p * (1 - p) + z * z)) / (2 * n + 2 * z * z)
    upper_score = (2 * n * p + z * z + z * math.sqrt(4 * n * p * (1 - p) + z * z)) / (2 * n + 2 * z * z)
    return max(0, min(1, lower_score)), max(0, min(1, upper_score))

if __name__ == "__main__":
    a = 1 
    b = 0 
    lower, upper = wilson_confidence(a, b, 0.67449)
    print "lower={0} upper={1}".format(lower, upper)
    lower, upper = wilson_confidence(a, b, 1.96)
    print "lower={0} upper={1}".format(lower, upper)
    lower, upper = wilson_confidence(a, b, 1.645)
    print "lower={0} upper={1}".format(lower, upper)
