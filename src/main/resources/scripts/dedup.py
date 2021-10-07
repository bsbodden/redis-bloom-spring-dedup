def runIt(x):
    val = x['value']['Value']
    name = x['value']['Name']
    execute('TS.INCRBY', 's-unfiltered', 1, 'TIMESTAMP', '*')
    j = execute("BF.ADD", "DEDUP", val)
    if j > 0:
        execute('TS.INCRBY', 's-filtered', 1, 'TIMESTAMP', '*')

gb = GB('StreamReader',desc="Process messages")
gb.map(runIt)
gb.register('outfill')
