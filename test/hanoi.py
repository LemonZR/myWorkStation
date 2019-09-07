#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 利用递归函数计算阶乘
# N! = 1 * 2 * 3 * ... * N
print "我是被调用模块: ",__name__
def fact(n):

    if n == 1:
        return 1
    return n * fact(n-1)



# 利用递归函数移动汉诺塔:
def move(n, a, b, c):
    #print __name__
    if n == 1:
        print('move', a, '-->', c)
    else:
        move(n-1, a, c, b)
        move(1, a, b, c)
        move(n-1, b, a, c)

if __name__=="__main__":
    #move(5, 'A', 'B', 'C')

    dict ={}
    for i  in  "abcabcab":
        for j,k in [2,4],:
            dict[i]= map(lambda (a,b):a+b,zip(dict.get(i,[0,0]),[j,k]))

    print dict

