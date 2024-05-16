#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# Copyright (C) converter3dji 2022 AKKODIS INGENIERIE PRODUIT SAS (support@realfusio.com)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#         http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


## DEPENDENCIES
import json, hashlib, base64, os, copy, requests, uuid, sys, math, datetime, re, io, logging, inspect, multiprocessing, psutil, subprocess, time, typing, binascii, http.server

########################################
#
# settings for Converter3dji
#
########################################
class Converter3djiSettings:
	def __init__(self):
		# cli directory nickname
		self.directoryNickName = None
		# projectid in which data should be uploaded, eg : prj_13e6a110322ce015a7ce890120ac0af9
		self.projectId = None
		# folder in which conversion result will be cached, eg : D:/converter3dji_cache/prj_13e6a110322ce015a7ce890120ac0af9
		self.cacheFolder = None
		# should we reprocess files that were in error
		self.reprocessCacheErrors = False
		# should we copy source file localy before processing ? will improve performances if files are stored in a network drive
		self.copyBeforeLoad = False
		# should we recall PsCustomizer on doc that are in cache
		self.reprocessDocFromCache = False
		# should we force reprocessing by the PsConverter, it will bypass local cache and PsConverter cache
		self.forceFileProcessing = False
		# how many time we should wait to get project lock before returning an error
		self.waitForProjectLockTimeOutSec = 30
		# InfiniteCli exe
		self.infiniteCliExe = None
		# keep serving in memory indexer for ever, usefull to debug generator cli
		self.inmemoryindexerserveforever = False
		# should we enable document validation in document indexer to speedup debug process
		self.inmemoryindexerenabledocumentvalidation = False
		# listening port of the document indexer
		self.docindexhttpport=8686
		# final scene unit
		self.outputunit='millimeter'

	# load settings from a dict
	def loadFromJson(self, pJson : dict):
		for k in pJson:
			if not hasattr(self,k):
				raise Exception('Unexpected setting key ' + k)
			setattr(self,k,pJson[k])

	def echo(self, pLogger : logging.Logger):
		lStr = '\nConverter3djiSettings : '
		for (name,value) in inspect.getmembers(self):
			if name.startswith('_') :
				continue
			if inspect.ismethod(value) or value is None: 
				continue
			if 'url' in name.lower():
				lCredentialsMatch = re.match(r'^(https?):\/\/(.+?):(.+?)@(.*)$',value)
				if not lCredentialsMatch is None and len(lCredentialsMatch.groups()) == 4:
					lStr = lStr + '\n\t' + name + ' = ' + str(lCredentialsMatch.groups()[0]) + '://****:****@' + str(lCredentialsMatch.groups()[3])
				continue
			lStr = lStr + '\n\t' + name + ' = ' + str(value)
		pLogger.info(lStr)

	# ensure settings validity
	def checkValidity(self):
		if not isinstance(self.directoryNickName, str):
			raise Exception('invalid directoryNickName')
		if not isinstance(self.projectId, str):
			raise Exception('invalid projectId')
		if not isinstance(self.cacheFolder, str):
			raise Exception('invalid cacheFolder')
		if not isinstance(self.reprocessCacheErrors, bool):
			raise Exception('invalid reprocessCacheErrors')
		if not isinstance(self.copyBeforeLoad, bool):
			raise Exception('invalid copyBeforeLoad')
		if not isinstance(self.reprocessDocFromCache, bool):
			raise Exception('invalid reprocessDocFromCache')
		if not isinstance(self.forceFileProcessing, bool):
			raise Exception('invalid forceFileProcessing')
		if not (isinstance(self.waitForProjectLockTimeOutSec, int) or isinstance(self.waitForProjectLockTimeOutSec, float)):
			raise Exception('invalid waitForProjectLockTimeOutSec')
		if not isinstance(self.infiniteCliExe, str):
			raise Exception('invalid infiniteCliExe')
		if not os.path.isfile(self.infiniteCliExe) or not os.path.exists(self.infiniteCliExe):
			raise Exception('invalid infiniteCliExe')
		if not isinstance(self.inmemoryindexerserveforever, bool):
			raise Exception('invalid inmemoryindexerserveforever')
		if not isinstance(self.inmemoryindexerenabledocumentvalidation,bool):
			raise Exception('invalid inmemoryindexerenabledocumentvalidation')
		if not isinstance(self.docindexhttpport,int):
			raise Exception('invalid docindexhttpport')
		if not self.outputunit in ['millimeter','centimeter','decimeter','meter','inch']:
			raise Exception('invalid outputunit')
		

########################################
#
# overload this class to customize PsConverter output
#
########################################
class PsCustomizer:
	def __init__(self, pLogger : logging.Logger):
		self.__mSpecialPropRe = re.compile(r'(.*)\:\:(.+)')
		self.__mLogger = pLogger
		self.__mParam = None
	
	def _setConverter3djiSettings(self, pSettings):
		self.__mParam = pSettings

	# use this method to customize extract settings per file
	# check PsConverter documentation for in depth description
	def computeExtractSettings(self, pFileName : str):
		lExt = os.path.splitext(pFileName)[1].lower()
		lSubPartLevel = ['root']
		if lExt in ['.fbx','.vrml','.gltf','.obj','.wrl','.wrz','.igs','.stp', '.step']:
			lSubPartLevel = ['geometry']
		elif lExt in ['.catpart','.cgr','.jt']:
			lSubPartLevel = ['assembly','component','geometricset']
		elif lExt in ['.3dxml']:
			lSubPartLevel = ['component', 'geometricset']
		
		lGeometryLevel = ['root']
		if lExt in ['.fbx']:
			lGeometryLevel = ['geometry']
		elif lExt in ['.catpart','.cgr','.jt']:
			lGeometryLevel = ['root']
		elif lExt in ['.catproduct']:
			lGeometryLevel = [
					"assembly",
					"part",
					"component",
					"geometricset"]

		return {
				'overridecolor':None,
				'extractannot':True,
				'experimentalextractannotcapture':True,
				'extractmetadata':True,
				'extractlinkmetadata':True,
				'extracthiddenobjects':pFileName.lower().endswith('catproduct'),
				'geometrylevel':lGeometryLevel,
				'geometrysettings':{
					'subgeometrylevel':lSubPartLevel,
					'computefeatures':True
					#,'backfaceculling': 'none'
				},
				'fixinvalidxforms':'auto',
				'computeaabb':False,
				'inputunit': 'auto',
				'outputunit': self.__mParam.outputunit
		}

	# this method will allow to update docs returned by the converter
	# default behavior will regroup some properties into sub objects
	def processConvResult(self, pDocsMap : typing.Dict[str,dict], pRootId : str, pSourceFilePath : str, pAABB):
		self.helperHandleBadXForm(pDocsMap)

		for docid in pDocsMap:
			lDoc = pDocsMap[docid]
			if not (lDoc['type'] in ['partmetadata','linkmetadata']) or not 'metadata' in lDoc:
				continue
			lMd = lDoc['metadata']

			# filter CoreTechno metadata which have default values
			self.helperRemoveGroupOfDefaultValues(lMd,[
				{"First Inertia Axis Xx":0.0,"First Inertia Axis Xy": 0.0,"First Inertia Axis Xz": 0.0,"Second Inertia Axis Yx": 0.0,"Second Inertia Axis Yy": 0.0,"Second Inertia Axis Yz": 0.0,"Third Inertia Axis Zx": 0.0,"Third Inertia Axis Zy": 0.0,"Third Inertia Axis Zz": 0.0},
				{"First Inertia Moment (kg*m2)":0.0,"Second Inertia Moment (kg*m2)":0.0,"Third Inertia Moment (kg*m2)":0.0},
				{"Inertia Matrix Ixx (kg*m2)": 0.0,"Inertia Matrix Ixy (kg*m2)": 0.0,"Inertia Matrix Iyy (kg*m2)": 0.0,"Inertia Matrix Iyz (kg*m2)": 0.0,"Inertia Matrix Izx (kg*m2)": 0.0,"Inertia Matrix Izz (kg*m2)": 0.0},
				{"Area (m2)": 0.0},
				{"Volume (m3)": 0.0},
				{"Mass (kg)": 0.0},
				{"Length (m)": 0.0},
				{"GX (m)": 0.0,"GY (m)": 0.0,"GZ (m)": 0.0},
				{"Xmin (m)": 0.0,"Ymin (m)": 0.0,"Zmin (m)": 0.0,"Xmax (m)": 0.0,"Ymax (m)": 0.0,"Zmax (m)": 0.0},
				{"Original mass unit (kg)": 1.0,"Original length unit (m)": 0.001,"Original time unit (s)": 1.0}
			])

			for k in ['CT_ID']:
				if k in lMd:
					del lMd[k]

			# regroup CoreTechno metadata
			self._regroupValues(lMd,["Volume Density (kg/m3)","Surface Density (kg/m2)","Linear Density (kg/m)","Original mass unit (kg)","Original length unit (m)","Original time unit (s)","First Inertia Moment (kg*m2)","Second Inertia Moment (kg*m2)","Third Inertia Moment (kg*m2)","Area (m2)","Volume (m3)","Mass (kg)","Length (m)","GX (m)","GY (m)","GZ (m)","First Inertia Moment (kg/m2)","Second Inertia Moment (kg/m2)","Third Inertia Moment (kg/m2)","Inertia Matrix Ixx (kg/m2)","Inertia Matrix Iyy (kg/m2)","Inertia Matrix Izz (kg/m2)","Inertia Matrix Ixy (kg/m2)","Inertia Matrix Iyz (kg/m2)","Inertia Matrix Izx (kg/m2)","First Inertia Axis Xx","First Inertia Axis Xy","First Inertia Axis Xz","Second Inertia Axis Yx","Second Inertia Axis Yy","Second Inertia Axis Yz","Third Inertia Axis Zx","Third Inertia Axis Zy","Third Inertia Axis Zz","Xmin (m)","Ymin (m)","Zmin (m)","Xmax (m)","Ymax (m)","Zmax (m)"],'PhysicalProperties')

			# look for XXXXXX::YYY
			lSpecificMd = dict()
			lToDelete = set()
			for k in lMd:
				lMatchRes = self.__mSpecialPropRe.match(k)
				if lMatchRes is None:
					continue
				lKey = lMatchRes.group(1)
				lValKey = lMatchRes.group(2)

				if not lKey in lSpecificMd:
					lSpecificMd[lKey] = dict()
				lSpecificMd[lKey][lValKey] = lMd[k]
				lToDelete.add(k)
			for k in lToDelete:
				del lMd[k]
			if len(lSpecificMd) > 0:
				lMd['SpecificMd'] = []
				for k in lSpecificMd:
					lMd['SpecificMd'].append( {'name':k,'values':lSpecificMd[k]})

	def helperHandleBadXForm(self, pDocsMap : typing.Dict[str,dict]):
		lNewLinkMdDocs = {}
		lTs = round(datetime.datetime.now().timestamp())
		for docid in pDocsMap:
			lDoc = pDocsMap[docid]
			if not (lDoc['type'] in ['structure']) or not 'children' in lDoc:
				continue
			for cid in lDoc['children']:
				lChild = lDoc['children'][cid]
				if not 'psconverter:badxform' in lChild:
					continue
				# create a link metadata if there is not
				lLinkMd = None
				if not 'linkmetadatadocuments' in lChild:
					lLinkMd = {
						"id":str(uuid.uuid4()),
						"type":"linkmetadata",
						"ts": lTs
					}
					lNewLinkMdDocs[lLinkMd['id']] = lLinkMd
					lChild['linkmetadatadocuments'] = [{'docid':lLinkMd['id']}]
				else:
					lLinkMd = pDocsMap[cid]
				if not 'metadata' in lLinkMd:
					lLinkMd['metadata'] = {}
				lLinkMd['metadata']['bad_xform'] = lChild['psconverter:badxform']
				del lChild['psconverter:badxform']

		for k in lNewLinkMdDocs:
			pDocsMap[k] = lNewLinkMdDocs[k]

	# this method will remove a group of metadata if they are all set to the default value
	# pGroupOfDefaultValues is an array of group {'key1':'defaultval1','key2':'defaultval2',...}
	def helperRemoveGroupOfDefaultValues(self, pMdObject : dict, pGroupOfDefaultValues):
		for grp in pGroupOfDefaultValues:
			lDiscard = True
			for (k,v) in grp.items():
				if not k in pMdObject or pMdObject[k] != v:
					lDiscard = False
					break
			if lDiscard:
				for k in grp:
					del pMdObject[k]

	# this method will rename given metadata keys
	def helperReMapMdKeys(self,pMdObject : dict,pMapping):
		for (k,k2) in pMapping.items():
			if not k in pMdObject:
				continue
			if k2 in pMdObject and pMdObject[k] != pMdObject[k2]:
				self.__mLogger.warning('metadata conflict while remaping %s=%s != %s=%s' % (k,pMdObject[k],k2,pMdObject[k2]))
			pMdObject[k2] = pMdObject[k]
			del pMdObject[k]

	def _regroupValues(self, pMd, pKeys, pDst):
		lGrp = dict()
		for lKey in pKeys:
			if lKey in pMd:
				lGrp[lKey] = pMd[lKey]
				del pMd[lKey]
		if len(lGrp) != 0:
			pMd[pDst] = lGrp

########################################
#
# XRefResolverInteface implement this interface to change the way xref are resolved
#
########################################
class XRefResolverInteface:
	def __init__(self):
		pass

	# pParentFilePath : file path of the file that contains pXRef
	# return an (absolute file path, convert priority) or None
	# entries with higher convert priority will be processed first
	def resolveXRef(self, pParentFilePath : str, pXRef : str):
		raise Exception('not implemented')

########################################
#
# class used convert files should output result compatible with PsConverter output
#
########################################
class ConverterJob:
	def __init__(self) -> None:
		self.mFilePath : str = None
		self.mUniqueId : str = None
		self.mForceConversion : bool = False
		self.mConvResultFilePath  : str = None
		self.mLogFilePath  : str = None
		self.mExtractSettings : dict = {}
		self.mCopyBeforeLoad : bool = False

class ConverterInterface:
	def __init__(self):
		pass
	# should return True if the job could be processed
	# pJob will be a PsConverter job alike, see PsConverter documentation for indepth details
	def pushJob(self, pJob : ConverterJob):
		raise Exception('not implemented')

	# should convert pending jobs
	def convert(self):
		raise Exception('not implemented')


########################################
#
# settings for Converter3dji
#
########################################
class PsConverterSettings:
	def __init__(self):
		# how many file to process concurrently, eg : 4
		self.workerCount = multiprocessing.cpu_count()
		# max memory to be shared between workers, eg : 2048
		self.maxRamMB = max(2048,psutil.virtual_memory().total / (self.workerCount * 1024 * 1024))
		# max processing time per job, eg : 120
		self.maxTimePerWorkerSec = 120
		# location of InfiniteCli.exe
		self.infiniteCliExe = None

	# load settings from a dict
	def loadFromJson(self, pJson : dict, pInfiniteCliExe : str):
		for k in pJson:
			if not hasattr(self,k):
				raise Exception('Unexpected setting key ' + k)
			setattr(self,k,pJson[k])
		self.infiniteCliExe = pInfiniteCliExe

	def echo(self, pLogger : logging.Logger):
		lStr = '\nPsConverterSettings : '
		for (name,value) in inspect.getmembers(self):
			if name.startswith('_') :
				continue
			if inspect.ismethod(value):
				continue
			if 'url' in name.lower():
				lCredentialsMatch = re.match(r'^(https?):\/\/(.+?):(.+?)@(.*)$',value)
				if not lCredentialsMatch is None and len(lCredentialsMatch.groups()) == 4:
					lStr = lStr + '\n\t' + name + ' = ' + str(lCredentialsMatch.groups()[0]) + '://****:****@' + str(lCredentialsMatch.groups()[3])
				continue
			lStr = lStr + '\n\t' + name + ' = ' + str(value)
		pLogger.info(lStr)

	# ensure settings validity
	def checkValidity(self):
		if not isinstance(self.infiniteCliExe, str):
			raise Exception('invalid infiniteCliExe')
		if not os.path.isfile(self.infiniteCliExe) or not os.path.exists(self.infiniteCliExe):
			raise Exception('invalid infiniteCliExe')
		if not isinstance(self.workerCount, int):
			raise Exception('invalid workerCount')
		if not isinstance(self.maxRamMB, int):
			raise Exception('invalid maxRamMB')
		if not isinstance(self.maxTimePerWorkerSec, int):
			raise Exception('invalid maxTimePerWorkerSec')

########################################
#
# a ConverterInterface base on PsConverter.exe
#
########################################
class PsConverter(ConverterInterface):
	def __init__(self, pPsConverterParam : PsConverterSettings, pConverter3djiParam : Converter3djiSettings, pLogger : logging.Logger):
		if not isinstance(pPsConverterParam, PsConverterSettings):
			raise Exception('Invalid pPsConverterParam')
		if not isinstance(pConverter3djiParam, Converter3djiSettings):
			raise Exception('Invalid pConverter3djiParam')
		pPsConverterParam.checkValidity()
		self.__mParams = pPsConverterParam
		self.__m3DJIParams = pConverter3djiParam
		self.__mConvCptr = 1
		self.__mJobCptr = 0
		self.__mLogger = pLogger
		self.__mJobFile = None
		os.makedirs(os.path.join(self.__m3DJIParams.cacheFolder,'psconvertercache'),exist_ok=True)
		os.makedirs(os.path.join(self.__m3DJIParams.cacheFolder,'generatorcache'),exist_ok=True)
		os.makedirs(os.path.join(self.__m3DJIParams.cacheFolder,'tmp_psconverter'),exist_ok=True)
		
		lPsConverterLogFile = os.path.join(self.__m3DJIParams.cacheFolder,'psconverter.log')
		if os.path.exists(lPsConverterLogFile):
			os.unlink(lPsConverterLogFile)

	def pushJob(self, job : ConverterJob):
		if self.__mJobFile is None:
			self.__mJobFile = open(os.path.abspath(os.path.join(self.__m3DJIParams.cacheFolder,'tmp_psconverter',str(self.__mConvCptr)+ '.x-ndjson')),'wb')
			lSettings = {'log':{
					'log2console':False,
					'enablediag':True,
					'folder': os.path.join(self.__m3DJIParams.cacheFolder)
					
				},
				'system':{
					'workercount':self.__mParams.workerCount,
					'threadperworker': 1,
					'maxrammb':self.__mParams.maxRamMB,
					'maxtimeperworkersec':self.__mParams.maxTimePerWorkerSec,
					'retryworkercount': 1 if self.__mParams.workerCount > 1 else 0,
					'geometrypoolid': self.__m3DJIParams.projectId.replace('prj_','geom_'),
					'tempfolder': os.path.join(self.__m3DJIParams.cacheFolder,'tmp_psconverter'),
					'generatorcachefolder':os.path.join(self.__m3DJIParams.cacheFolder,'generatorcache'),
				}
			}
			self.__mJobFile.write( (json.dumps(lSettings) + '\n').encode('utf-8') )
		
		lJob = {**{
					'file':job.mFilePath,
					'uniqueid':job.mUniqueId,
					'forceconversion':job.mForceConversion ,
					'convresult':job.mConvResultFilePath,
					'logfile':job.mLogFilePath,
					
				},**job.mExtractSettings}
		lJob['copybeforeload'] = job.mCopyBeforeLoad
		self.__mJobFile.write( (json.dumps(lJob) + '\n').encode('utf-8') )
		self.__mJobCptr = self.__mJobCptr + 1
		return True

	def convert(self):
		if self.__mJobFile is None:
			return
		lJobFileName = self.__mJobFile.name
		self.__mJobFile.close();
		self.__mJobFile = None
		self.__mConvCptr = self.__mConvCptr + 1
		self.__mLogger.info('call PsConverter with %i jobs' % (self.__mJobCptr))
		self.__mJobCptr = 0
	
		lCmdLine = [os.path.abspath(self.__m3DJIParams.infiniteCliExe), 'psconverter','convert', lJobFileName,self.__m3DJIParams.directoryNickName]
		self.__mLogger.debug('Execute : "' + '" "'.join(lCmdLine) + '"')
		lRes = subprocess.run(lCmdLine,stdout = None, stderr = None, cwd=os.path.split(os.path.abspath(self.__m3DJIParams.infiniteCliExe))[0])
		if lRes.returncode != 0:
			self.__mLogger.error('Error %i while running "%s"' % (lRes.returncode,'" "'.join(lCmdLine)))
			raise Exception()


########################################
#
# default implementation of XRefResolverInteface this class to change the way xref are resolved
#
########################################
class FileSystemXRefResolver(XRefResolverInteface):
	def __init__(self, pBaseDir : str, pCacheFile : str, pLogger : logging.Logger, pIgnoreFileWithSameSize = False):
		XRefResolverInteface.__init__(self)

		self.__mLogger = pLogger
		# map : file name => array(relpath,filesize)
		self.__mFilePathMap = dict()
		self.__mBaseDir = os.path.abspath(pBaseDir)
		self.__mIgnoreFileWithSameSize = pIgnoreFileWithSameSize

		lCptr = 0
		lCacheIsValid = False
		if pCacheFile is None:
			self.__mLogger.debug('FileSystemXRefResolver no cache file specified')
		elif not os.path.isfile(pCacheFile):
			self.__mLogger.debug('FileSystemXRefResolver cache file is missing')
		elif (os.stat(pBaseDir).st_mtime > os.stat(pCacheFile).st_mtime):
			self.__mLogger.debug('FileSystemXRefResolver base folder ts is newer than cache file')
		else:
			lCacheContent = {}
			with open(pCacheFile,'r',encoding='UTF-8') as f:
				lCacheContent = json.load(f)
			if not 'sourcefolder' in lCacheContent or lCacheContent['sourcefolder'] != pBaseDir:
				self.__mLogger.warn('FileSystemXRefResolver cache file was build from an other base dir, rebuild it')
			else:
				self.__mFilePathMap = lCacheContent['files']
				lCacheIsValid = True
				self.__mLogger.info('FileSystemXRefResolver load index of %s from cache %s ' % (pBaseDir,pCacheFile))
				for f in self.__mFilePathMap:
					lCptr = lCptr + len(self.__mFilePathMap[f])
				

		if not lCacheIsValid:
			self.__mLogger.info('FileSystemXRefResolver start indexing ' + pBaseDir)
			for (dirpath, dirnames, filenames) in os.walk(self.__mBaseDir):
				lCptrStart = lCptr
				self.__mLogger.debug('%s ...' % (dirpath)) 
				for lFile in filenames:
					lFullFilePath = os.path.join(dirpath,lFile)
					lRelativePath = self.__normalizePath(lFullFilePath)
					lExt = os.path.splitext(lFullFilePath)[1].lower()
					if lExt in ['.catproduct', '.jt', '.catpart', '.cgr', '.model', '.fbx', '.obj', '.gltf', '.plmxml', '.vrml', '.wrl', '.wrz', '.igs', '.stp', '.step','.3dxml']:
						lFileName = os.path.basename(lFullFilePath)
						lFileNameLowered = lFileName.lower()
						lSize = os.path.getsize(lFullFilePath)
						if lFileNameLowered in self.__mFilePathMap:
							lSkipFile = False
							if self.__mIgnoreFileWithSameSize:
								for f,s in self.__mFilePathMap[lFileName]:
									if s == lSize:
										lSkipFile = True
										break
							if not lSkipFile:
								self.__mFilePathMap[lFileNameLowered].append((lRelativePath,lSize))
						else:
							self.__mFilePathMap[lFileNameLowered] = [(lRelativePath,lSize)]
						lCptr = lCptr + 1
				if lCptr !=lCptrStart:
					self.__mLogger.debug('%s : %d files indexed' % (dirpath,lCptr-lCptrStart)) 

			if not pCacheFile is None:
				if not os.path.isdir(os.path.dirname(pCacheFile)):
					os.makedirs(os.path.dirname(pCacheFile))
				with open(pCacheFile,'w',encoding='UTF-8') as f:
					lCacheContent = {
						'files':self.__mFilePathMap,
						'sourcefolder': pBaseDir
					}
					json.dump(lCacheContent,f,sort_keys=True,indent='\t')
		self.__mLogger.info('FileSystemXRefResolver is ready with %d files ' % (lCptr) )

	def __iter__(self):
		for (k,vals) in self.__mFilePathMap.items():
			for v in vals:
				yield os.path.join(self.__mBaseDir,v[0])

	def resolveXRef(self,pParentFilePath : str , pXRef : str):
		lXRef = pXRef.replace('\\','/')
		lFileName = os.path.basename(lXRef)
		lFileNameLowered = lFileName.lower()
		if not lFileNameLowered in self.__mFilePathMap:
			self.__mLogger.warning("Fail to resolve xref " + lXRef + " unknown file")
			return None
		else:
			# get relative file path of parent
			lParentRelPath = self.__normalizePath(os.path.dirname(pParentFilePath))

			lRelPathList = self.__mFilePathMap[lFileNameLowered]
			lMatch = []

			# first we look for a file which is in a subfolder of parent
			if len(lMatch) == 0:
				for (p,size) in lRelPathList:
					if p.startswith(lParentRelPath) and lXRef.endswith(p):
						lMatch.append((p,size))

			# second make a global search
			lPathLengthMatched = len(lFileName)
			if len(lMatch) == 0:
				for (p,size) in lRelPathList:
					for i in range(len(p),lPathLengthMatched,-1):
						if lXRef.endswith(p[-i:]):
							if i > lPathLengthMatched:
								lMatch.clear()
								lPathLengthMatched = i
							elif i < lPathLengthMatched:
								break
							lMatch.append((p,size))
							break
			if len(lMatch) == 0:
				lMatch = lRelPathList
			if len(lMatch) > 1:
				self.__mLogger.warning("multiple path match for xref "+lXRef+", choosing one at random")
			lMatch = lMatch[0]
			self.__mLogger.debug('resolve "%s" to "%s" from "%s"' % (pXRef,lMatch[0],pParentFilePath))
			return (os.path.join(self.__mBaseDir,lMatch[0]),lMatch[1])

	def __normalizePath(self, pPath : str):
		lRes = os.path.relpath(pPath,self.__mBaseDir).replace('\\','/')
		if lRes == '.':
			lRes = ''
		return lRes




########################################
#
# internal class used to aggregate metadata types
# and propose a default mapping
#
########################################
class MetadataTypeMapping:
	def __init__(self, pLogger : logging.Logger) -> None:
		self.__mMdTypes = {}
		self.__mLogger = pLogger

	def addMetadataBlock(self, pMdBlock : dict):
		self.__recurseOnMd(pMdBlock,[],'')

	def __addType(self, pPath, pType):
		lPath = '.'.join(pPath)
		if not lPath in self.__mMdTypes:
			self.__mMdTypes[lPath] = set()
		self.__mMdTypes[lPath].add(pType)

	def __recurseOnMd(self, pMd, pPath, pTypePrefix):
		if pMd is None:
			# skip it
			return
		elif type(pMd) is dict:
			self.__addType(pPath,pTypePrefix+'object')
			for k in pMd:
				self.__recurseOnMd(pMd[k],pPath + [k],'')
		elif type(pMd) is str:
			if 'date' in pPath[-1].lower():
				self.__addType(pPath,pTypePrefix+'date')
			else:
				self.__addType(pPath,pTypePrefix+'text')
		elif type(pMd) is int:
			self.__addType(pPath,pTypePrefix+'integer')
		elif type(pMd) is float:
			self.__addType(pPath,pTypePrefix+'double')
		elif type(pMd) is bool:
			self.__addType(pPath,pTypePrefix+'boolean')
		elif pMd is None:
			# self.__addType(pPath,pTypePrefix+'boolean')
			pass
		elif type(pMd) is list:
			for e in pMd:
				self.__recurseOnMd(e,pPath,pTypePrefix+'list_of_')
		else:
			raise Exception('Unhandled mapping type %s for %s' % (type(pMd),pPath))

	def getMapping(self):
		self.__mLogger.info('About to create mapping from %s' % self.__mMdTypes)
		lStr = 'Found metadata keys :'
		lRootProperties = {}
		for k in sorted(self.__mMdTypes.keys()):
			if k == '':
				continue
			lPath = k.split('.')
			lTypes = self.__mMdTypes[k]
			for t in ['text','integer','double','date','boolean']:
				lListOfT='list_of_' + t
				if t in lTypes and lListOfT in lTypes:
					lTypes.remove(lListOfT)
			if 'integer' in lTypes and 'double' in lTypes:
				lTypes.remove('integer')
			if len(lTypes) != 1:
				self.__mLogger.warn('detect several types for metadata %s : %s' % (k,self.__mMdTypes[k]))
				continue
			lType = list(self.__mMdTypes[k])[0]
			lStr = lStr + '\n\t%s : %s' % (k,lType)
			if lType in ['text','list_of_text','integer','list_of_integer','double','list_of_double','date','list_of_date','boolean','list_of_boolean','object']:
				# this is a basic type
				self.__createMappingEntry(lRootProperties,lPath,lType.replace('list_of_',''),lPath)
			elif lType == 'list_of_object':
				self.__createMappingEntry(lRootProperties,lPath,'nested',lPath)
			else:
				self.__mLogger.warn('unhandled metadata type %s : %s' % (k,lType))
				continue
		lProposedMapping = {
			"id": "com.3djuump:indexmapping",
			"type": "projectdocument",
			"subtype": "indexmapping",
			"metadatamapping":{
				"dynamic":False,
				"properties":lRootProperties
			},
			"dynamic_templates": [
			],
			"ts": 1
		}
		lStr = lStr + "\nproposed mapping : " + json.dumps(lProposedMapping)
		if len(self.__mMdTypes) > 128:
			self.__mLogger.warn('detect a huge number of metadata keys, you might have indexing issues, consider reducing it')

		self.__mLogger.info(lStr)
		return lProposedMapping

	def __createMappingEntry(self,pMapping, pPath, pType, pFullPath):
		if not pPath[0] in pMapping:
			pMapping[pPath[0]] = {}
		lDstObj = pMapping[pPath[0]]
		if len(pPath) > 1:
			if not 'properties' in lDstObj:
				if 'type' in lDstObj and not lDstObj['type'] in ['nested','object']:
					self.__mLogger.warn('fail to create mapping entry for %s, missing properties field for %s' % (pFullPath,pPath[0]))
					return
				lDstObj['properties'] = {}
			self.__createMappingEntry(lDstObj['properties'],pPath[1:],pType,pFullPath)
		else:
			if not 'type' in lDstObj:
				lDstObj['type'] = pType
				if pType == 'date':
					lDstObj['ignore_malformed'] = True
					lDstObj['format'] = "date_optional_time||dd/MM/yyyy"
				elif pType in ['nested','object']:
					lDstObj['dynamic'] = False
			elif lDstObj['type'] != pType:
				self.__mLogger.warn('fail to create mapping entry for %s, type conflict' % (pFullPath))
				return


########################################
#
# this class is responsible to walk throught ps and convert it
# after conversion results will be uploaded to the generator
#
# /!\ this class SHOULD be used in a with ... as ... statement
#
########################################
class Converter3dji:
	def __init__(self, pParam : Converter3djiSettings, pCustomizer : PsCustomizer, pXRefSolver : XRefResolverInteface, pConverters, pLogger : logging.Logger):
		if not isinstance(pParam, Converter3djiSettings):
			raise Exception('Invalid pParam')
		if not isinstance(pCustomizer, PsCustomizer):
			raise Exception('Invalid pCustomizer')
		if not pXRefSolver is None and not isinstance(pXRefSolver, XRefResolverInteface):
			raise Exception('Invalid pXRefSolver')


		self.__mLogger = pLogger
		self.__mParam = pParam
		self.__mParam.checkValidity()

		self.__mCustomizer = pCustomizer
		self.__mCustomizer._setConverter3djiSettings(pParam)
		self.__mConverters = pConverters
		self.__mXRefSolver = pXRefSolver
		self.__mRemainingFilesToProcess = dict()
		self.__mAllProcessedFiles = set()
		self.__mPotentialRootFiles = set()
		self.__mDocumentIndexer = DocumentIndexer(pParam,pLogger)
		self.__mAllMdKeys = MetadataTypeMapping(pLogger)
		self.__mTriggerBuildWasCalled = False

	def __enter__(self):

		lCmdLine = [os.path.abspath(self.__mParam.infiniteCliExe), 'generator','canbuild', self.__mParam.projectId, self.__mParam.directoryNickName]
		
		self.__mLogger.debug('Execute : "' + '" "'.join(lCmdLine) + '"')
		lRes = subprocess.run(lCmdLine, cwd=os.path.split(os.path.abspath(self.__mParam.infiniteCliExe))[0])
		if lRes.returncode != 0:
			self.__mLogger.error('Error %i while running "%s"' % (lRes.returncode,'" "'.join(lCmdLine)))
			raise Exception()

		return self

	def __exit__(self, exc_type, exc_value, traceback):
			if exc_type is None:
			self.__mAllMdKeys.getMapping()
		
		# if there was no exception or exception occurs during build process, keep indexer alive
		if (exc_type is None or self.__mTriggerBuildWasCalled) and self.__mParam.inmemoryindexerserveforever:
			print('\n#######################')
			print('\n#######################')
			print('Keep docindexer alive on http://127.0.0.1:%d/docindexer/api , Hit CTRL+C to stop it' % self.__mParam.docindexhttpport)
			print('#######################')
			print('#######################')
			print('Doc indexer dump will be available in : %s' % os.path.join(self.__mParam.cacheFolder,'tmp_generation','docindexer.dump'))
			while True:
				time.sleep(0.1)
		self.__mDocumentIndexer.onEvent('cleanup')
	
	def getDocIndexerAdapter(self):
		return self.__mDocumentIndexer

	# this method will send a build request to the server
	def triggerBuild(self,  pBuildParameters : dict, pWorkerCount : int, pTmpFolder : str = None):
		self.__mTriggerBuildWasCalled = True
		lWorkingFolder = pTmpFolder if not pTmpFolder is None else os.path.join(self.__mParam.cacheFolder,'tmp_generation',pBuildParameters['buildcomment'])

		os.makedirs(lWorkingFolder,exist_ok=True)
		# generate build job file
		lBuildJob = {
			'build':{
				'buildparameters' : pBuildParameters,
				'geometrypoolid':self.__mParam.projectId.replace('prj_','geom_'),
				'projectid':self.__mParam.projectId 
			},
			'log':{
				'log2console':False,
				'enablediag':True
			},
			'generatorcachefolder':os.path.join(self.__mParam.cacheFolder,'generatorcache'),
			'system':{
				'workercount':pWorkerCount,
				'workingfolder': lWorkingFolder
			}
		}
		lBuildInfoFile = os.path.join(lWorkingFolder,'generation_job.json')
		with open(lBuildInfoFile,'w',encoding='utf-8') as f:
			json.dump(lBuildJob,f,indent=4)

		self.__mDocumentIndexer.onEvent('start_build')

		lCmdLine = [os.path.abspath(self.__mParam.infiniteCliExe), 'generator','build', lBuildInfoFile, 'http://127.0.0.1:%d/docindexer/api' % self.__mParam.docindexhttpport, self.__mParam.directoryNickName]
		self.__mLogger.debug('Execute : "' + '" "'.join(lCmdLine) + '"')
		with subprocess.Popen(lCmdLine,stdout = None, stderr = None, cwd=os.path.split(os.path.abspath(self.__mParam.infiniteCliExe))[0]) as lGenerationProcess:
			lGenerationReturnCode = None
			while lGenerationReturnCode is None:
				try:
					lGenerationReturnCode = lGenerationProcess.wait(0.01)
					if lGenerationReturnCode != 0:
						self.__mLogger.error('Error %i while running "%s"' % (lGenerationReturnCode,'" "'.join(lCmdLine)))
						# do not raise exception here maibe we need to keep document indexer alive
					else:
						self.__mLogger.info('Generation done')
				except subprocess.TimeoutExpired:
					pass
				self.__mDocumentIndexer.onEvent('update_build')

		self.__mDocumentIndexer.onEvent('end_build')

		if lGenerationReturnCode != 0:
			raise Exception()
		

	# use this method to upload documents
	# if pInput is a folder path, all json files will be added
	# if pInput is a file path, document will be loaded from file (ts will be updated based on file last modified date)
	# if pInput could be a dict representing the document
	def addDocument(self, pInput, ts=None):
		if isinstance(pInput,dict):
			self.__mDocumentIndexer.addDocument(pInput)
		elif isinstance(pInput,list):
			for j in pInput:
				self.addDocument(j,ts)
		elif os.path.isdir(pInput):
			for file in os.listdir(pInput):
				if file.endswith('.json'):
					self.addDocument(os.path.join(pInput,file),ts)
				else:
					self.__mLogger.debug('ignore non json file ' + file)
		elif os.path.isfile(pInput) and pInput.endswith('.json'):
			try:
				with open(pInput,'r',encoding='utf-8') as f:
					lJson = json.load(f)
					self.addDocument(lJson,ts=self._getFileTs(pInput))
			except:
				self.__mLogger.exception('Fail to load ' + pInput)
				raise Exception('Fail to load %s error was %s'%(pInput,sys.exc_info()))
		else:
			self.__mLogger.critical('Unknown input ' + str(pInput))
			raise Exception('Unknown input ' + str(pInput))

	# use this method to upload client customization script
	def addClientScript(self, pFilePath, pTaskScripts):
		with open(pFilePath,'r',encoding='utf-8') as f:
			lScript = f.read()
		scriptdoc = {
				'id':'com.3djuump:scripts',
				'type':'projectdocument',
				'subtype':'scripts',
				'scriptbase64' : base64.b64encode(lScript.encode('utf-8')).decode('ascii'),
				'taskscripts' : {},
			'ts': self._getFileTs(pFilePath)
		}
		for tasktype in pTaskScripts:
			with open(pTaskScripts[tasktype],'r',encoding='utf8') as f:
				taskscript = f.read()
			scriptdoc['taskscripts'][tasktype] = base64.b64encode(taskscript.encode('utf-8')).decode('ascii')
			scriptdoc['ts'] = max(scriptdoc['ts'],self._getFileTs(pTaskScripts[tasktype]))
		
		self.__mDocumentIndexer.addDocument(scriptdoc)

	def getDefaultBuildParameters(self):
		return {
			"rootstructuredocid" : "...",
			"applicableconfigurations":None,
			"limitpstoconfigurations":True,
			"tags":[],
			"xformtolerance" : {
				"translation" : 0.01,
				"rotation" : 0.001
					},
			"lowdeftrlcount" : 1000000,
			"buildcomment" : "Build comment",
			"visiblercount" : 20000,
			"modelaabblimit" : {
				"xmin" : -3.3e38,
				"xmax" : 3.3e38,
				"ymin" : -3.3e38,
				"ymax" : 3.3e38,
				"zmin" : -3.3e38,
				"zmax" : 3.3e38
					},
			'unit':self.__mParam.outputunit,
			'modelframe':{
				'up':[0.0,0.0,1.0],
				'front':[1.0,0.0,0.0]
			}
		}

	# deprecated method use convertFiles
	def convert(self,pRootFiles : typing.List[str], pGenerateTopNode : bool = True):
		lRes = self.convertFiles(pRootFiles,pGenerateTopNode)
		lRes2 = []
		for k in lRes:
			lRes2.append(lRes[k])
		return lRes2

	# call this method to process product structure
	# this method will return list of generated root ids
	def convertFiles(self,pRootFiles, pGenerateTopNode : bool = True):
		lTimeStart = time.time()
		self.__mLogger.info('Start processing')

		lRootFiles = pRootFiles
		if not isinstance(lRootFiles,list):
			if not isinstance(lRootFiles,str):
				self.__mLogger.critical('input should be list or str')
				raise Exception('input should be list or str')
			lRootFiles = [lRootFiles]
		if len(lRootFiles) == 0:
			self.__mLogger.critical('need at least one root file')
			raise Exception('need at least one root file')
		lGenerateTopNode = pGenerateTopNode and len(lRootFiles) > 1

		self.__mLogger.info('Convert %i root file%s, %s top node' % (len(lRootFiles), 's' if len(lRootFiles) > 1 else '', 'with' if lGenerateTopNode else 'without'))

		self.__mAllProcessedFiles = set()
		self.mRemainingFilesToProcess = dict()
		for r in lRootFiles:
			lRootFile = os.path.abspath(r)
			self.__mRemainingFilesToProcess[lRootFile] = 0.
			self.__mAllProcessedFiles.add(lRootFile)
			self.__mPotentialRootFiles.add(lRootFile)

		while len(self.__mRemainingFilesToProcess) > 0:
			lToConvert : typing.List[ConverterJob] = []
			# iterate over self.mRemainingFilesToProcess until it is not empty files
			# could have been added during loop if an entry was already in the cache
			while len(self.__mRemainingFilesToProcess) > 0:
				lCurrentBatch = []
				for k in self.__mRemainingFilesToProcess:
					lCurrentBatch.append((k,self.__mRemainingFilesToProcess[k]))
				lCurrentBatch.sort(key=lambda x:-x[1])
				self.__mRemainingFilesToProcess = dict()
				for (lBatchEntry,lWeight) in lCurrentBatch:
					(lFileHash,lCacheFolder,lConvResultFile) = self._computeFileInfo(lBatchEntry)
					os.makedirs(lCacheFolder,exist_ok=True)
					
					lForceFileConversion = self.__mParam.forceFileProcessing
					if not lForceFileConversion and self.__mParam.reprocessCacheErrors:
						lConvResult = self._loadJsonFile(lConvResultFile)
						if 'infos' in lConvResult and 'errors' in lConvResult['infos'] and len(lConvResult['infos']['errors']) > 1:
							self.__mLogger.info('force reprocess of ' + lConvResultFile)
							lForceFileConversion = True

					if lForceFileConversion:
						# clear cache to ensure that we will not process an old file
						for fc in os.listdir(lCacheFolder):
							file_path = os.path.join(lCacheFolder, fc)
							if os.path.isfile(file_path):
								os.unlink(file_path)

						# need to reprocess file
					lJob = ConverterJob()
					lJob.mFilePath = lBatchEntry
					lJob.mUniqueId = lFileHash
					lJob.mForceConversion = lForceFileConversion
					lJob.mConvResultFilePath = os.path.abspath(lConvResultFile + '.ori')
					lJob.mLogFilePath = os.path.abspath(lConvResultFile + '.ori')[:-4] + '.log'
					lJob.mExtractSettings = self.__mCustomizer.computeExtractSettings(lBatchEntry)
					lJob.mCopyBeforeLoad = self.__mParam.copyBeforeLoad is not None and self.__mParam.copyBeforeLoad
					lToConvert.append(lJob)
						
					
			self.__mLogger.info('Send %i files to PsConverter ' % (len(lToConvert)))

			# call converters
			if len(lToConvert) == 0:
				continue
			for job in lToConvert:
				pushed = False
				for converter in self.__mConverters:
					if converter.pushJob(job):
						pushed = True
						break
				if not pushed:
					self.__mLogger.warning("No converter for job "+str(job))
			for converter in self.__mConverters:
				converter.convert()

			# analyze results
			lUpdateFilesCounter = 0
			lAnalyzedFilesCounter = 0
			for job in lToConvert:
				(lFileHash,lCacheFolder,lConvResultFile) = self._computeFileInfo(job.mFilePath)
				if not os.path.isfile(lConvResultFile + '.ori'):
					self.__mLogger.error('Fail to retrieve convert result ''%s'' ''%s''' % (job.mFilePath, lFileHash))
					continue

				lNeedToReprocess = True
				lFileEtag = os.stat(lConvResultFile+ '.ori').st_mtime
				# compare file etags to know if psconverter as updated the file or not
				if os.path.exists(lConvResultFile):
					lConvResultFileTs = os.stat(lConvResultFile).st_mtime
					if lFileEtag <= lConvResultFileTs:
						lNeedToReprocess = self.__mParam.reprocessDocFromCache
					else:
						lUpdateFilesCounter = lUpdateFilesCounter + 1
				else:
					lUpdateFilesCounter = lUpdateFilesCounter + 1

				if lNeedToReprocess:
					lConvResult = self._loadJsonFile(lConvResultFile+ '.ori')
					self._callPsCustomizer(lConvResult,lFileHash,job.mFilePath,lConvResult['infos']['ts'])
					# save convresult it might have been modified by ps customizer
					with open(lConvResultFile,'w',encoding='UTF-8') as of:
						json.dump(lConvResult,of,indent='\t')
					lAnalyzedFilesCounter = lAnalyzedFilesCounter + 1
				else:
					lConvResult = self._loadJsonFile(lConvResultFile)
				
				self._analyzeconvresult(job.mFilePath,lFileHash,lConvResult)
				
			self.__mLogger.info('PsConverter has updated %i/%i files' % (lUpdateFilesCounter,len(lToConvert)))
			self.__mLogger.info('Customization was applied to %i/%i files' % (lAnalyzedFilesCounter,len(lToConvert)))
		
		self.__mLogger.debug('Root files : ' + json.dumps(list(self.__mPotentialRootFiles)))

		lRootIds = {}
		if lGenerateTopNode:
			lRootIds[''] = 'root'
			lRootDoc = {
				'id':'root',
				'type':'structure',
				'children' : {}
			}
			for r in self.__mPotentialRootFiles:
				(lChildId,_,_) = self._computeFileInfo(r)
				lRootDoc['children']['root_' + lChildId] = { 'ref':lChildId }
			self.__mDocumentIndexer.addDocument(lRootDoc)
		else:
			for r in self.__mPotentialRootFiles:
				(lRootId,_,_) = self._computeFileInfo(r)
				lRootIds[r] = lRootId
		self.__mLogger.debug('Processing done in : %d sec' % (time.time() - lTimeStart))
		self.__mLogger.debug('Rood documents : ' + json.dumps(lRootIds))
		return lRootIds

	def _callPsCustomizer(self, pConvResult : str, pRootId : str, pSourceFilePath : str, pTs : int, pIncrementTs=False):
		lIndexedDocs = dict()
		for d in pConvResult['docs']:
			lIndexedDocs[d['id']] = d

		if not pRootId in lIndexedDocs:
			if not 'infos' in pConvResult:
				pConvResult['infos'] = {}
			if not 'errors' in pConvResult['infos']:
				pConvResult['infos']['errors'] = []
			lError = 'root document is missing from converter result'
			if not lError in pConvResult['infos']['errors']:
				pConvResult['infos']['errors'].append(lError)
			return

		lAabb = None
		if 'infos' in pConvResult:
			if 'aabb' in pConvResult['infos']:
				lAabb = pConvResult['infos']['aabb']

		self.__mCustomizer.processConvResult(lIndexedDocs,pRootId,pSourceFilePath,lAabb)

		if not pRootId in lIndexedDocs:
			if not 'info' in pConvResult:
				pConvResult['info'] = {}
			if not 'errors' in pConvResult['info']:
				pConvResult['info']['errors'] = []
			lError = 'root document was removed by ps customizer'
			if not lError in pConvResult['infos']['errors']:
				pConvResult['info']['errors'].append(lError)
			return

		pConvResult['docs']=[]
		for k in lIndexedDocs:
			if not 'ts' in lIndexedDocs[k]:
				lIndexedDocs[k]['ts'] = pTs
			if pIncrementTs:
				lIndexedDocs[k]['ts'] = lIndexedDocs[k]['ts'] + 1
			pConvResult['docs'].append(lIndexedDocs[k])

	def _loadJsonFile(self, pFileName : str):
		try:
			with open(pFileName,'r',encoding='utf-8') as f:
				return json.load(f)
		except:
			return {}

	def _getFileTs(self, pFileName : str):
		return round(os.path.getmtime(pFileName))

	def _computeFileInfo(self,pFileName : str):
		m = hashlib.sha256()
		m.update(pFileName.encode('utf8'))
		lUniqueId = binascii.hexlify(m.digest()).decode('ascii')
		lUniqueId = lUniqueId.replace('/','_')
		lUniqueId = os.path.split(pFileName)[1] + '_' + lUniqueId
		return ('struct_' + lUniqueId, 
			os.path.join(self.__mParam.cacheFolder,'psconvertercache',lUniqueId), 
			os.path.join(self.__mParam.cacheFolder,'psconvertercache',lUniqueId,'convresult.json'))
	
		
	def _analyzeconvresult(self, pParentFilePath : str , pParentHash : str, pConvResult : str):
		
		if 'infos' in pConvResult and 'errors' in pConvResult['infos']:
			for e in pConvResult['infos']['errors']:
				self.__mLogger.error('error %s (%s) => %s' % (pParentFilePath,pParentHash,e))
		if 'infos' in pConvResult and 'warnings' in pConvResult['infos']:
			for w in pConvResult['infos']['warnings']:
				self.__mLogger.warning('warnings %s (%s) => %s' % (pParentFilePath,pParentHash,w))

		# look for xrefs
		lXRefs = dict()
		for lDoc in pConvResult['docs']:
			lFinalDoc = lDoc

			if lDoc['type'] == 'structure' and 'children' in lDoc:
				# make a deep copy, we don't want to alter original data from psconverter to be able to adjust 
				# xref resolution if new files appears/disappears
				lFinalDoc = copy.deepcopy(lDoc)
				lFinalDoc['children'] = {}
				for c in lDoc['children']:
					lChild = copy.deepcopy(lDoc['children'][c])
					if 'psconverter:xref' in lChild:
						lXRef = None
						if not self.__mXRefSolver is None:
							lXRef = self.__mXRefSolver.resolveXRef(pParentFilePath,lChild['psconverter:xref'])
						del lChild['psconverter:xref']
						if not lXRef is None:
							# do not use os.path.realpath this is really slow !!
							lXRefRealPath = os.path.abspath(lXRef[0])
							(lChild['ref'],_,_) = self._computeFileInfo(lXRefRealPath)
							lXRefs[lXRefRealPath] = lXRef[1]
						else:
							# link to a missing structure document to generate an error
							lChild['ref'] = 'unresolved_xref_dummy_struct_doc'
					lLinkId = c
					if 'psconverter:xrefmetadata' in lChild:
						lLinkMdDoc = {}
						# create a link metadata, a better solution would be to set those metadata on xref root but here we have no knowlage of it and we might need to ensure that this extra metadata block is the same each time this xref is used
						# for now don't bother to add metadata on an existing document
						if not 'linkmetadatadocuments' in lChild:
							lLinkId = lDoc['id'] + '_link_' + c
							lChild['linkmetadatadocuments'] = [{'docid':lLinkId}]
							lLinkDoc = {
								'id' : lLinkId,
								'type':'linkmetadata',
								'metadata': lChild['psconverter:xrefmetadata'],
								'ts':pConvResult['infos']['ts']
							}
							self.__mDocumentIndexer.addDocument(lLinkDoc)
						del lChild['psconverter:xrefmetadata']
					lFinalDoc['children'][lLinkId] = lChild
			elif lDoc['type'] in ['partmetadata','linkmetadata','instancemetadata'] and 'metadata' in lDoc:
				for k in lDoc['metadata']:
					self.__mAllMdKeys.addMetadataBlock(lDoc['metadata'])
			self.__mDocumentIndexer.addDocument(lFinalDoc)
		lXRefsSet = set(lXRefs.keys())
		for k in ( lXRefsSet - self.__mAllProcessedFiles):
			self.__mRemainingFilesToProcess[k] = lXRefs[k]
		self.__mAllProcessedFiles = self.__mAllProcessedFiles | lXRefsSet
		self.__mPotentialRootFiles = self.__mPotentialRootFiles - lXRefsSet


########################################
#
# add 'set' support in json streaming
#
########################################
class SetEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, set):
			return list(obj)
		return json.JSONEncoder.default(self, obj)
########################################
#
# class used by Converter3dji to interact with document indexer
#
########################################
class DocumentIndexer():
	def __init__(self, pParams : Converter3djiSettings, pLogger : logging.Logger):
		self.__mLogger = pLogger
		self.__mCurrentBatch = io.BytesIO()
		self.__mCurrentBatchDocCount = 0
		self.__mParam = pParams
		self.__mUrlBase = 'http://127.0.0.1:%s/docindexer/api' % pParams.docindexhttpport

		lLogFile = os.path.join(self.__mParam.cacheFolder , 'docindexer.log')
		if os.path.isfile(lLogFile):
			os.unlink(lLogFile)
		lCmdLine = [
			os.path.abspath(self.__mParam.infiniteCliExe),
			'generator','docindexer', '%d' % pParams.docindexhttpport,
			'--compress', '--close-with-parent', '--log-file', lLogFile,
			'--dump-out',os.path.join(self.__mParam.cacheFolder,'tmp_generation','docindexer.dump')
			]
		self.__mLogger.debug('Execute : "' + '" "'.join(lCmdLine) + '"')
		self.__mSubProcess = subprocess.Popen(lCmdLine,stdout = None, stderr = None, cwd=os.path.split(os.path.abspath(self.__mParam.infiniteCliExe))[0])

		self.__mPool = requests.Session()
		
	def addDocument(self, pDoc : dict, ts: int = None):
		# our script has added ts on all documents remove those that should not be here
		if pDoc['type'] in ['structure','annotation','conf']:
			if 'ts' in pDoc:
			del pDoc['ts']
		elif ts is not None:
			pDoc['ts'] = ts
		elif not 'ts' in pDoc:
			pDoc['ts'] = round(datetime.datetime.now().timestamp())
			
		lToAppend = ('{"id":%s,"type":%s,"ts":%d}\n' % (json.dumps(pDoc['id']),json.dumps(pDoc['type']),pDoc['ts'] if 'ts' in pDoc else 1)).encode('utf8')
		lToAppend = lToAppend + json.dumps(pDoc,cls=SetEncoder).encode('utf8') + b'\n'

		if ( (self.__mCurrentBatch.getbuffer().nbytes + len(lToAppend)) > 80*1024*1024 or 
			self.__mCurrentBatchDocCount >= 10000
			):
			self.__uploadBatch()
		self.__mCurrentBatch.write(lToAppend)
		self.__mCurrentBatchDocCount = self.__mCurrentBatchDocCount + 1

	def onEvent(self, pEvent : str):
		if pEvent == 'start_build':
			self.__uploadBatch()
		elif pEvent == 'update_build':
			pass
		elif pEvent == 'end_build':
			pass
		elif pEvent == 'cleanup':
			if not self.__mSubProcess is None:
				self.__mSubProcess.kill()
				del self.__mSubProcess
				self.__mSubProcess = None
		else:
			raise Exception()

	def __uploadBatch(self):
		# check if document indexer is still running
		lIsIndexerRunning = False
		try:
			self.__mSubProcess.wait(0.01)
		except subprocess.TimeoutExpired:
			lIsIndexerRunning = True
		if not lIsIndexerRunning:
			self.__mLogger.critical('Document indexer is not running !')
			raise Exception()
		if self.__mCurrentBatch.getbuffer().nbytes == 0:
			return
		lUploadStart = time.time()
		self.__mLogger.debug('Upload %s document(s) for %sB to document index' % (self.__mCurrentBatchDocCount,self.__mCurrentBatch.getbuffer().nbytes))
		lToSend = self.__mCurrentBatch.getvalue()
		with open(self.__mParam.cacheFolder + '/lastesbatch.txt','wb') as f:
				f.write(lToSend)
		lUrl = self.__mUrlBase + '/_index' + ( '?enabledocumentvalidation=true' if self.__mParam.inmemoryindexerenabledocumentvalidation else '')
		lResponse = self.__mPool.post(lUrl, data=lToSend, headers={"Content-Type": "application/x-ndjson"}, proxies={"http": "","https": ""})
			if lResponse.status_code != 200:
			self.__mLogger.critical('Document indexer error, Invalid return code for _index\n' + str(lResponse.status_code) + '\n' + lResponse.reason + '\n' + str(lResponse.text))
			raise Exception();
		self.__mLogger.debug('Inserted %i docs in the index in %i sec' % (self.__mCurrentBatchDocCount,time.time() - lUploadStart))
		self.__mCurrentBatch = io.BytesIO()
		self.__mCurrentBatchDocCount = 0
	
