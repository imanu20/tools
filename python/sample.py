#!/usr/bin/python
#-*-coding:utf-8-*-

import random
import sys

def sample(sampling_list,sampling_num,sample_num,sample_str):
	"""
	流式采样，从不定长的输入中随机抽取N个
	@para sampling_list: 采样的输出结果
	@para sampling_num: 需要采样的个数
	@para sample_num:   当前流式输入中样本的个数
	@para input_str:    输入的内容

	计算方法:
	 if sample_num <= sampling_num,
		 则样本直接被选中
	 else:
		 生成0-1的随机数
		 if 随机数>= samplint_num/sample_num #表示该样本有sampling_num/sample_num的概率被选中
			 continue
		 else:
			 则当前样本被选中，并随机替换之前的被选中的样本
	详见:http://blog.csdn.net/goooxu/article/details/6311686		 

	"""
	len_sampling_list = len(sampling_list)
	if len_sampling_list < sampling_num:
		sampling_list.append(sample_str)
	else:
		proba = random.random()
		if proba >= (float)(sampling_num)/sample_num:
			#print "not select:[%d %d]"%(sample_num,sample_str)
			pass
		else:
			#随机挑选一个已经被选择的样本，然后用当前样本替换之
			_index = random.randint(0,sampling_num-1)
			sampling_list[_index] = sample_str

if  __name__ == "__main_stdin__":
    sample_num = (int)(sys.argv[1])
    output_file = sys.argv[2]

    sample_list = []
    count = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        count += 1
        sample(sample_list,sample_num,count,line)

    fs_out = file(output_file,"w")
    for ele in sample_list:
        fs_out.write("%s\n"%(ele))
    fs_out.close()    
if  __name__ == "__main__":
    input_file = sys.argv[1]
    sample_num = (int)(sys.argv[2])
    output_file = sys.argv[3]

    fs_in = file(input_file)
    sample_list = []
    count = 0
    for line in fs_in:
        line = line.strip()
        if not line:
            continue
        count += 1
        sample(sample_list,sample_num,count,line)
    fs_in.close()

    fs_out = file(output_file,"w")
    for ele in sample_list:
        fs_out.write("%s\n"%(ele))
    fs_out.close()    


if __name__ == "__main2__":
	a = [i for i in range(0,100)]
	count = 0;
	sample_list = []
	for ele in a:
		count += 1
		sample(sample_list,10,count,ele)
	for ele in sample_list:
		print ele
