package cn.dazhiyy.distributed.lock.exception;

/**
 * @author dazhi
 * @projectName distributedlock
 * @packageName cn.dazhiyy.distributed.lock.exception
 * @className DistributedException
 * @description TODO
 * @date 2019/4/17 22:10
 */
public class DistributedException extends Exception {

    public DistributedException(String msg){
        super(msg);
    }
}
