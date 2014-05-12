import subprocess

def duplicate_counter(fileName, guid_pos):
	guid_counts = {}
	with open(fileName, 'r') as f:
		for l in f:
			guid = l.split(",")[guid_pos]
			guid_counts[guid] = guid_counts.get(guid,0) + 1
	dups = [val-1 for val in guid_counts.values() if val > 1]	
	total_dups = sum(dups)
	max_guid = max(guid_counts.iterkeys(),key=(lambda key:guid_counts[key]))
	return total_dups,max_guid,guid_counts[max_guid]
	
if __name__ == "__main__":
	#directory, guid pos
	sdir,sgpos = "/usr/local/var/ftp_sync/downloaded/Standard/",0
	cdir,cgpos = "/usr/local/var/ftp_sync/downloaded/Conversion/",1
	rdir,rgpos = "/usr/local/var/ftp_sync/downloaded/Rich/",0

	for fdir, gpos in [[sdir,sgpos], [cdir,cgpos], [rdir,rgpos]]:
		files = subprocess.check_output(['ls', fdir])
		files = files.split()
		for f in files:
			total_dups, max_uid, max_rep = duplicate_counter(fdir + f, gpos)
			if max_rep > 1:
				print ("%s\t"*4)%(f,total_dups,max_uid, max_rep)
			
		
