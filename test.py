from etl.teaminternet import main as teaminternet

try:
    teaminternet.run()
except ValueError as error:
    print('Caught this error: ' + repr(error))
