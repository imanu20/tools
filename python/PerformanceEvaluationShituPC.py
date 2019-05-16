#! /bin/env python
import pystreaming
import hashlib 
import math
import sys
import yaml

class PerformanceEvaluation(pystreaming.MapReduce):
    nreduce=1
    precision=10000
    relative_error_bound=0.05
    max_span=0.01
    encoding="latin"


    def map(self,line):
        parts=line.split('\t')
        cmatch = int(parts[9])
        rank = int(parts[12])
        is_ppim = (cmatch == 204) and ( rank >= 0 and rank < 5)
        is_pp = (cmatch == 225) and ( rank >= 0 and rank < 9)
        pageNo = int(parts[47])
        is_url_ad = int(parts[231])
        if pageNo != 0:
            return
        if is_url_ad != 1:
            return
        if cmatch != 225 or rank != 1:
            return
        if not ( is_pp or is_ppim):
            return
        cookied = parts[7]
        if cookied == "00000000000000000000000000000000":
            return 
        show = float(parts[0])
        click = float(parts[1])
        if is_pp: 
            ctr = float(parts[88])/1e6
        else:
            ctr = float(parts[89])/1e6
        if ctr >=0.9999:
            ctr=0.9999
        if ctr <=1e-6:
            ctr = 1e-6
        mse=(show-click)*(ctr**2)+click*((1-ctr)**2)
        log_loss=math.log(ctr)*click+(show-click)*math.log(1-ctr)
        yield int(math.floor(ctr*self.precision)),show,click,ctr*show,mse,log_loss

    def combine(self,key,value_iter):
        result=reduce(lambda x,y:map(lambda u,v:u+v,x,y),value_iter)
        yield key,result


    def reduce_init(self):
        self.result_cache=[]
        self.result={}

    def reduce(self,key,value_iter):
        result=reduce(lambda x,y:map(lambda u,v:u+v,x,y),value_iter)
        show,click,ctr_sum,mse,log_loss=result
        ctr=ctr_sum/show
        self.result_cache.append([ctr,show,click,mse,log_loss])
        return
        yield

    def calculate_predictor_average(self):
        data_tuple = reduce(lambda x,y:(x[0]+y[0]*y[1],x[1]+y[1]),self.result_cache,(0,0))
        predictor_average = data_tuple[0]/data_tuple[1]
        sum_result = result=reduce(lambda x,y:map(lambda u,v:u+v,x,y),self.result_cache)
        poster_average = sum_result[2]/sum_result[1]
        return predictor_average,poster_average

    def reduce_end(self):
        self.result_cache.sort(reverse=True)
        auc=self.calculate_auc()
        mse=self.calculate_mse()
        log_loss=self.calculate_logloss()
        abs_error,log_error=self.calculate_correlation()
        bias = self.calculate_bias();
        predictor_ctr,actual_ctr = self.calculate_predictor_average()
        self.result['bias'] = bias
        self.result['auc']=auc
        self.result['mse']=mse
        self.result['abs_error']=abs_error
        self.result['log_error']=log_error
        self.result['log_loss']=log_loss
        self.result['predictor_ctr'] = predictor_ctr
        self.result['actual_ctr']=actual_ctr
        self.result['data']=filter(lambda x:x!=None,self.result_cache)
        sys.stdout.write(yaml.dump(self.result))

    def calculate_bias(self):
        total_scorce_sum =reduce(lambda x,y:x+y[0]*y[1],filter(lambda x:x!=None,self.result_cache),0)
        total_click =reduce(lambda x,y:x+y[2],filter(lambda x:x!=None,self.result_cache),0)
        return total_scorce_sum/total_click

    def calculate_auc(self):
        total_sum=reduce(lambda x,y:map(lambda u,v:u+v,x,y),filter(lambda x:x!=None,self.result_cache))
        total_click=total_sum[2]
        total_nonclick=total_sum[1]-total_click
        print >>sys.stderr,total_click,total_nonclick
        click_sum=0.0
        nonclick_sum=0.0
        auc=0.0
        last_x=0.0
        last_y=0.0
        self.result['auc_point']=[]
        for ctr,show,click,mse,log_loss in self.result_cache:
            click_sum+=click
            nonclick_sum+=show-click
            y=click_sum/total_click
            x=nonclick_sum/total_nonclick
            auc+=(y+last_y)/2*(x-last_x)
            self.result['auc_point'].append([x,y])
            last_x=x
            last_y=y
        x=1.0
        y=1.0
        auc+=(y+last_y)/2*(x-last_x)
        return auc

    def calculate_logloss(self):
        total_sum=reduce(lambda x,y:map(lambda u,v:u+v,x,y),filter(lambda x:x!=None,self.result_cache))
        return total_sum[4]/total_sum[1]

    def calculate_mse(self):
        total_sum=reduce(lambda x,y:map(lambda u,v:u+v,x,y),filter(lambda x:x!=None,self.result_cache))
        return total_sum[3]/total_sum[1]

    def calculate_correlation(self):
        last_ctr=-1
        impression_sum=0
        ctr_sum=0.0
        click_sum=0.0
        error_sum=0.0
        log_error_sum=0.0
        error_count=0
        self.result['ctr_point']=[]
        for ctr,show,click,mse,log_loss in self.result_cache:
            if abs(ctr-last_ctr)>self.max_span:
                last_ctr=ctr
                impression_sum=0.0
                ctr_sum=0.0
                click_sum=0.0
            impression_sum+=show
            ctr_sum+=ctr*show
            click_sum+=click
            adjust_ctr=ctr_sum/impression_sum
            relative_error=math.sqrt((1-adjust_ctr)/(adjust_ctr*impression_sum))
            if relative_error<self.relative_error_bound:
                actual_ctr=click_sum/impression_sum
                relative_ctr_error=abs(actual_ctr/adjust_ctr-1)
                self.result['ctr_point'].append([adjust_ctr,actual_ctr])
                log_relative_ctr_error=abs(math.log(actual_ctr/adjust_ctr+1e-10))
                error_sum+=relative_ctr_error*impression_sum
                log_error_sum+=log_relative_ctr_error*impression_sum
                error_count+=impression_sum
                last_ctr= -1
        if error_count>0:
            return error_sum/error_count,math.exp(log_error_sum/error_count)-1
        else:
            return 0,0


if __name__=="__main__":
    PerformanceEvaluation().launch()
