class Log(object):

	def __init__(self):
		self.disableTime = True

		self.warningsCount = 0
		self.criticalsCount = 0

	def warning(self, msg):
		self.warningsCount += 1
		self.onMessage("[WARNING] ", msg)

	def critical(self, msg):
		self.criticalsCount += 1
		self.onMessage("[CRITICAL] ", msg)

	def msg(self, msg):
		self.onMessage("", msg)

	def getWarningsCount(self):
		return self.warningsCount

	def onMessage(self, prefix, msg):
		if self.disableTime:
			print "%s%s" % (prefix, msg)
		else:
			print "%s%s %s" % (prefix, datetime.now().time(), msg)