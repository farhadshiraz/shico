package com.shico.cassandra.profiling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class ProfilerAspect {
	private static final Logger logger = LoggerFactory.getLogger(ProfilerAspect.class);
	
	private static ConcurrentMap<String, Pair<Long, Long>> timers = new ConcurrentHashMap<String, Pair<Long, Long>>();
	private final ReentrantLock lock = new ReentrantLock();
		
	@Around("@annotation(profiler) && within(com.shico.cassandra.statistic..*)")
	public Object measureTime(ProceedingJoinPoint pjp, Profiler profiler) throws Throwable {
		String timer = profiler.value();
		
		long start = System.currentTimeMillis();
		Object proceed = pjp.proceed();
		long time = System.currentTimeMillis() - start;
		
		lock.lock();
		try{
			long total = (timers.get(timer) == null ? 0L : timers.get(timer).getValue0());
			long numOfInvocations = (timers.get(timer) == null ? 1L : timers.get(timer).getValue1());
			timers.put(timer, new Pair<Long, Long>(time+total, ++numOfInvocations));
		}catch(Exception e){
			logger.warn("Could not calculate time for the execution of method."+e.getMessage());
		}finally{
			lock.unlock();
		}
		return proceed;
	}

	public static ConcurrentMap<String, Pair<Long, Long>> getTimers() {
		return timers;
	}
}
