#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8


import  threading
def out(i,j,k=3):


    tmp = 'sad_gg'.split("_")
    if len(tmp) == 3:
        channel, pay_method, order_type = tmp
    elif len(tmp) == 2:
        channel, order_type = tmp
        pay_method = 'empty'

    else:
        print "wrong"
    return channel,pay_method,order_type
class a(threading.Thread):
    def run(self):
        print "wo run le "
    pass

def run_me(instance,*args):
    instance.run()

if __name__ == "__main__":

    # r = runMe()
    # r.add_work(out,1)
    # r.add_work(out,2)
    # r.add_work(out,3)
    # r.jion()
    #print out(1,3)
    a= a()
    run_me(a)
