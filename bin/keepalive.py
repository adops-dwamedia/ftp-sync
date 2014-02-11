import os

t = 0
interval = 10
while True:
	os.system("sleep %sm"%interval)
	t += interval
	print "%s min"%t 
