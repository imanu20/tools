#!/usr/bin/python
#-*-coding: utf-8-*-
import sys

def scoreClickAUC(num_clicks, num_impressions, predicted_ctr):
    i_sorted = sorted(range(len(predicted_ctr)),key=lambda i: predicted_ctr[i],
                      reverse=True)
    auc_temp = 0.0
    click_sum = 0.0
    old_click_sum = 0.0
    no_click = 0.0
    no_click_sum = 0.0

    # treat all instances with the same predicted_ctr as coming from the same bucket
    last_ctr = predicted_ctr[i_sorted[0]] + 1.0

    for i in range(len(predicted_ctr)):
        if last_ctr != predicted_ctr[i_sorted[i]]:
            auc_temp += (click_sum+old_click_sum) * no_click / 2.0
            old_click_sum = click_sum
            no_click = 0.0
            last_ctr = predicted_ctr[i_sorted[i]]
        no_click += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]
        no_click_sum += num_impressions[i_sorted[i]] - num_clicks[i_sorted[i]]
        click_sum += num_clicks[i_sorted[i]]
    auc_temp += (click_sum+old_click_sum) * no_click / 2.0
    auc = auc_temp / (click_sum * no_click_sum)
    return auc

def mse(clicks, ctrs):
    if len(clicks) != len(ctrs):
        return 0
    mse = 0.0
    for i in range(len(ctrs)):
        mse += math.pow(clicks[i] - ctrs[i], 2)
    return mse* 1.0 / len(ctrs)

if __name__ == "__main__":
    
    f = open(sys.argv[1])
    clicks = []
    impress = []
    ctrs = []
    for line in f:
        tokens = line.strip().split(" ")
        try: 
            click = int(tokens[0])
            if click > 0:
                click = 1
            p = float(tokens[1])
        except:
            pass
        clicks.append(click)
        ctrs.append(p)
        impress.append(1)
    print scoreClickAUC(clicks, impress, ctrs)
