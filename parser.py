f = open("zipCodeData.txt","r")

fileData = f.readlines()

finalZipCodes = {}
zipCodeList = []
for line in fileData:
	if "<li>" in line:
		index_start_li = line.find("<li>")+5
		if "</li>" in line:
			index_end_li = line.find("</li>")
			zipCode_desc = line[index_start_li:index_end_li]
			if "-" in zipCode_desc:
				zc_split = zipCode_desc.split("-")
				if "omitted" not in zc_split[1]:
					finalZipCodes[zc_split[0]] = zc_split[1]
					zipCodeList.append(zc_split[0])

for y in finalZipCodes:
	print y
g = open("resultZipCodeData.txt","w")
for x in zipCodeList:
	g.write(str(x).strip()+",")
f.close()
g.close()	
