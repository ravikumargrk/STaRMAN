allDataStr = input(">:")
if 'END' in allDataStr:
    allDataStr = allDataStr[0: allDataStr.index('END')]

allDataList = allDataStr.split(' ')

data = {
    'names': [],
    'scores': []
}

index = 0
for item in allDataList:
    if not len(item.replace(' ', '')):
        continue
    try:
        value = int(item)
        data['scores'].append(value)
    except ValueError:
        data['names'].append(item)

minScore = min(data['scores'])
minScoreIndex = data['scores'].index(minScore)
minScoreName = data['names'][minScoreIndex]

print(f'minimum score is {minScore} by {minScoreName}')

maxScoreLimit = minScore + 5
topNamesCount = 0

for score in data['scores']:
    if score <= maxScoreLimit:
        topNamesCount += 1
ans = round(100*topNamesCount/len(data["names"]), 2)
print(f'Second answer: {ans} %')