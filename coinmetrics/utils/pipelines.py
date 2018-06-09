import concurrent.futures
import traceback
import threading
import time
from datetime import datetime
from event import EventEmitter

class AtomicCounter(object):

	def __init__(self, value):
		self.value = value
		self.lock = threading.Lock()

	def inc(self):
		with self.lock:
			self.value += 1

	def get(self):
		return self.value


class LinearMultithreadedPipeline(object):

	def __init__(self, workersCount, proc, name):
		self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=workersCount)
		self.proc = proc
		self.workersCount = workersCount
		self.stopSignal = None
		self.name = name
		self.exceptions = []
		self.onExecuted = EventEmitter()

	def __str__(self):
		return self.name

	def pushTask(self, task, taskIndex):
		self.executor.submit(self._executeTask, task, taskIndex)

	def shutdown(self):
		self.executor.shutdown()

	def faulted(self):
		return len(self.exceptions) > 0

	def _executeTask(self, task, taskIndex):
		if not self.stopSignal():
			try:
				result = self.proc(task, self.stopSignal)
				self.onExecuted.trigger(result, taskIndex)
			except:
				self.exceptions.append((taskIndex, traceback.format_exc()))


class OrderingConnector(object):

	def __init__(self, source, destination):
		self.awaitingIndex = 0
		self.source = source
		self.destination = destination
		self.lock = threading.Lock()
		self.results = {}

		self.source.onExecuted.subscribe(self._onSourceReceived)

	def _onSourceReceived(self, result, taskIndex):
		with self.lock:
			self.results[taskIndex] = result
			while self.awaitingIndex in self.results:
				self.destination.pushTask(self.results[self.awaitingIndex], self.awaitingIndex)
				del self.results[self.awaitingIndex]
				self.awaitingIndex += 1


class ExecutionCounter(object):

	def __init__(self, pipeline, taskCount):
		self.pipeline = pipeline
		self.count = AtomicCounter(0)
		self.taskCount = taskCount
		self.pipeline.onExecuted.subscribe(self._onExecuted)

	def allDone(self):
		return self.count.get() == self.taskCount

	def getCount(self):
		return self.count.get()

	def _onExecuted(self, result, taskIndex):
		self.count.inc()


class EtaOutput(object):

	def __init__(self, pipeline, eta):
		self.eta = eta
		self.eta.workStarted()
		self.lock = threading.Lock()
		pipeline.onExecuted.subscribe(self._onExecuted)

	def _onExecuted(self, result, taskIndex):
		with self.lock:
			self.eta.workFinished(1)
			self.eta.workStarted()


class TaskSpawner(object):

	def __init__(self, tasks, pipeline, targetProc, spawnSignal):
		self.tasks = tasks
		self.pipeline = pipeline
		self.total = len(self.tasks)
		self.spawned = 0
		self.keyboardInterrupt = False
		self.targetProc = targetProc
		self.lock = threading.Lock()
		self.spawnSignal = spawnSignal
		self.spawnSignal.subscribe(self._onSpawnSignal)

	def interrupted(self):
		return self.keyboardInterrupt

	def run(self, stopSignal):
		try:
			self._onSpawnSignal()
			while not stopSignal():
				time.sleep(0.2)
		except KeyboardInterrupt:
			self.keyboardInterrupt = True

	def _onSpawnSignal(self, *args):
		with self.lock:
			target = min(self.total, self.targetProc())
			while self.spawned < target:
				self.pipeline.pushTask(self.tasks[self.spawned], self.spawned)
				self.spawned += 1


def reportPipelineExceptions(pipelines, tasks, taskRepr):
	for pipeline in pipelines:
		if pipeline.faulted():
			print ""
			print "[CRITICAL] Exceptions during execution of pipeline %s:" % pipeline
			for taskIndex, exception in pipeline.exceptions:
				if taskRepr is None:
					print "task index %d:" % taskIndex
				else:
					print "task %s" % taskRepr(tasks[taskIndex])
				print "%s" % exception


def runPipelineChain(tasks, pipelines, eta=None, taskRepr=None, prefetchCount=1):
	def anyPipelineFaulted():
		for pipeline in pipelines:
			if pipeline.faulted():
				return True
		return False

	def shouldStop():
		return counter.allDone() or taskSpawner.interrupted() or anyPipelineFaulted()
			
	counter = ExecutionCounter(pipelines[-1], len(tasks))

	if eta is not None:
		EtaOutput(pipelines[-1], eta)

	for pipeline in pipelines:
		pipeline.stopSignal = shouldStop

	taskSpawner = TaskSpawner(tasks, pipelines[0], lambda: counter.getCount() + prefetchCount, pipelines[-1].onExecuted)
	taskSpawner.run(shouldStop)

	for pipeline in pipelines:
		print "shutting down pipeline %s" % pipeline
		pipeline.shutdown()

	reportPipelineExceptions(pipelines, tasks, taskRepr)
	return counter.allDone() and not anyPipelineFaulted(), taskSpawner.interrupted()
