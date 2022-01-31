#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# Copyright (C) converter3dji 2022 AKKA INGENIERIE PRODUIT (support@realfusio.com)
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
import json, hashlib, base64, os, copy, requests, glob, sys, math, datetime, re, io, urllib, logging, inspect, multiprocessing, psutil, subprocess


########################################
#
# settings for Converter3dji
#
########################################
class Converter3djiSettings:
	def __init__(self):
		# url to proxy api, eg : https://login:pwd@host:443/proxy
		self.proxyApiUrl = None
		# projectid in which data should be uploaded, eg : prj_13e6a110322ce015a7ce890120ac0af9
		self.projectId = None
		# folder in which conversion result will be cached, eg : D:/converter3dji_cache/prj_13e6a110322ce015a7ce890120ac0af9
		self.cacheFolder = None
		# should we clear connector index before processing
		self.clearConnectorIndex = False
		# should we reprocess files that were in error
		self.reprocessCacheErrors = False
		# http proxy, eg : http://myproxy:9090
		self.httpProxy = None
		# should we copy source file localy before processing ? will improve performances if files are stored in a network drive
		self.copyBeforeLoad = False
		# should we accept self signed ssl certificates
		self.verifySSL = True
		# should we recall PsCustomizer on doc that are in cache
		self.reprocessDocFromCache = False
	
	# load settings from a dict
	def loadFromJson(self, pJson):
		for k in pJson:
			if not hasattr(self,k):
				raise Exception('Unexpected setting key ' + k)
			setattr(self,k,pJson[k])
	
	def echo(self, pLogger):
		lStr = '\nConverter3djiSettings : '
		for (name,value) in inspect.getmembers(self):
			if name.startswith('_') :
				continue
			if inspect.ismethod(value): 
				continue
			if 'url' in name.lower():
				lCredentialsMatch = re.match(r'^https:\/\/(.+?):(.+?)@(.*)$',self.proxyApiUrl)
				if not lCredentialsMatch is None and len(lCredentialsMatch.groups()) == 3:
					lStr = lStr + '\n\t' + name + ' = https://****:****@' + str(lCredentialsMatch.groups()[2])
				continue
			lStr = lStr + '\n\t' + name + ' = ' + str(value)
		pLogger.info(lStr)
	
	# ensure settings validity
	def checkValidity(self):
		if not isinstance(self.proxyApiUrl, str):
			raise Exception('invalid proxyApiUrl')
		if not isinstance(self.projectId, str):
			raise Exception('invalid projectId')
		if not isinstance(self.cacheFolder, str):
			raise Exception('invalid cacheFolder')
		if not isinstance(self.clearConnectorIndex, bool):
			raise Exception('invalid clearConnectorIndex')
		if not isinstance(self.reprocessCacheErrors, bool):
			raise Exception('invalid reprocessCacheErrors')
		if not isinstance(self.httpProxy, str) and not self.httpProxy is None:
			raise Exception('invalid httpProxy')
		if not isinstance(self.copyBeforeLoad, bool):
			raise Exception('invalid copyBeforeLoad')
		if not isinstance(self.reprocessDocFromCache, bool):
			raise Exception('invalid reprocessDocFromCache')
		if not isinstance(self.verifySSL, bool):
			raise Exception('invalid verifySSL')
		if not self.proxyApiUrl.endswith('/proxy'):
			raise Exception('Invalid proxyApiUrl, it should end with /proxy')

########################################
#
# overload this class to customize PsConverter output
#
########################################
class PsCustomizer:
	def __init__(self, pLogger):
		self.__mSpecialPropRe = re.compile(r'(.*)\:\:(.+)')
		self.__mLogger = pLogger
		pass
	
	# use this method to customize extract settings per file
	# check PsConverter documentation for in depth description
	def computeExtractSettings(self, pFileName):
		return {
				'extractannot':True,
				'extractannotoriginaldata':False,
				'extractmetadata':True,
				'extractlinkmetadata':True,
				'extracthiddenobjects':pFileName.lower().endswith('catproduct'),
				'subpartlevel':['root']
				}

	# this method will allow to update docs returned by the converter
	# default behavior will regroup some properties into sub objects
	def processConvResult(self, pDocsMap, pRootId, pSourceFilePath):
		for k in pDocsMap:
			lDoc = pDocsMap[k]
			if not (lDoc['type'] in ['partmetadata','linkmetadata']) or not 'metadata' in lDoc:
				continue
			lMd = lDoc['metadata']
			
			# regroup CoreTechno metadata 
			self._regroupValues(lMd,["Area (m2)","Volume (m3)","Mass (kg)","Length (m)","GX (m)","GY (m)","GZ (m)","First Inertia Moment (kg/m2)","Second Inertia Moment (kg/m2)","Third Inertia Moment (kg/m2)","Inertia Matrix Ixx (kg/m2)","Inertia Matrix Iyy (kg/m2)","Inertia Matrix Izz (kg/m2)","Inertia Matrix Ixy (kg/m2)","Inertia Matrix Iyz (kg/m2)","Inertia Matrix Izx (kg/m2)","First Inertia Axis Xx","First Inertia Axis Xy","First Inertia Axis Xz","Second Inertia Axis Yx","Second Inertia Axis Yy","Second Inertia Axis Yz","Third Inertia Axis Zx","Third Inertia Axis Zy","Third Inertia Axis Zz","Xmin (m)","Ymin (m)","Zmin (m)","Xmax (m)","Ymax (m)","Zmax (m)"],'MassProperties')
			self._regroupValues(lMd,["Original mass unit (kg)","Original length unit (m)","Original time unit (s)"],'OriginalUnits')
			self._regroupValues(lMd,["Volume Density (kg/m3)","Surface Density (kg/m2)","Linear Density (kg/m)"],'Density')
			
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
	# entries will higher convert priority will be processed first
	def resolveXRef(self,pParentFilePath,pXRef):
		raise Exception('not implemented')

########################################
#
# class used convert files should output result compatible with PsConverter output
#
########################################
class ConverterInterface:
	def __init__(self):
		pass
	# should return True if the job could be processed
	# pJob will be a PsConverter job alike, see PsConverter documentation for indepth details
	def pushJob(self, pJob):
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
		# url to directory api, eg : https://login:pwd@host:443/directory
		self.directoryApiUrl = None
		# how many file to process concurrently, eg : 4
		self.workerCount = multiprocessing.cpu_count()
		# max memory per worker, eg : 2048
		self.maxRamPerWorkerMB = max(2048,psutil.virtual_memory().total / (self.workerCount * 1024 * 1024))
		# max processing time per job, eg : 120
		self.maxTimePerWorkerSec = 120
		# location of PsConverter.exe
		self.psConverterExe = None
	
	# load settings from a dict
	def loadFromJson(self, pJson):
		for k in pJson:
			if not hasattr(self,k):
				raise Exception('Unexpected setting key ' + k)
			setattr(self,k,pJson[k])
	
	def echo(self, pLogger):
		lStr = '\nPsConverterSettings : '
		for (name,value) in inspect.getmembers(self):
			if name.startswith('_') :
				continue
			if inspect.ismethod(value): 
				continue
			if 'url' in name.lower():
				lCredentialsMatch = re.match(r'^https:\/\/(.+?):(.+?)@(.*)$',self.directoryApiUrl)
				if not lCredentialsMatch is None and len(lCredentialsMatch.groups()) == 3:
					lStr = lStr + '\n\t' + name + ' = https://****:****@' + str(lCredentialsMatch.groups()[2])
				continue
			lStr = lStr + '\n\t' + name + ' = ' + str(value)
		pLogger.info(lStr)
	
	# ensure settings validity
	def checkValidity(self):
		if not isinstance(self.directoryApiUrl, str):
			raise Exception('invalid directoryApiUrl')
		if not isinstance(self.psConverterExe, str):
			raise Exception('invalid psConverterExe')
		if not os.path.isfile(self.psConverterExe) or not os.path.exists(self.psConverterExe):
			raise Exception('invalid psConverterExe')
		if not isinstance(self.workerCount, int):
			raise Exception('invalid workerCount')
		if self.directoryApiUrl[-1] == "/":
			raise Exception('Invalid directoryApiUrl, it should end with /directory')
		if not isinstance(self.maxRamPerWorkerMB, int):
			raise Exception('invalid maxRamPerWorkerMB')
		if not isinstance(self.maxTimePerWorkerSec, int):
			raise Exception('invalid maxTimePerWorkerSec')

########################################
#
# a ConverterInterface base on PsConverter.exe
#
########################################
class PsConverter(ConverterInterface):
	def __init__(self, pPsConverterParam, pConverter3djiParam, pLogger):
		if not isinstance(pPsConverterParam, PsConverterSettings):
			raise Exception('Invalid pPsConverterParam')
		if not isinstance(pConverter3djiParam, Converter3djiSettings):
			raise Exception('Invalid pConverter3djiParam')
		pPsConverterParam.checkValidity()
		self.__mParams = pPsConverterParam
		self.__m3DJIParams = pConverter3djiParam
		self.__mConvCptr = 0
		self.__mJobs = []
		self.__mLogger = pLogger
		self.__mConverterLog = None
	def pushJob(self, job):
		self.__mJobs.append(job)
		return True
	def convert(self):
		if len(self.__mJobs) == 0:
			return
		task = {
			'jobs': self.__mJobs,
			'system':{
				'workercount':self.__mParams.workerCount,
				'maxramperworkermb':self.__mParams.maxRamPerWorkerMB,
				'maxtimeperworkersec':self.__mParams.maxTimePerWorkerSec,
				'directoryurl':self.__mParams.directoryApiUrl,
				'verify_ssl_peer':self.__m3DJIParams.verifySSL,
				'http_proxy': self.__m3DJIParams.httpProxy
			}
		}
		self.__mConvCptr = self.__mConvCptr + 1
		lConvFile = os.path.abspath(os.path.join(self.__m3DJIParams.cacheFolder,str(self.__mConvCptr)+ '.json'))
		with open(lConvFile,'w') as f:
			json.dump(task,f,sort_keys=True)
		lCmdLine = [os.path.abspath(self.__mParams.psConverterExe), "-convert", lConvFile]
		
		if self.__mConverterLog is None:
			self.__mConverterLog = open(os.path.join(self.__m3DJIParams.cacheFolder,'PsConverter.log'),'w')
		
		lRes = subprocess.run(lCmdLine,stdout = self.__mConverterLog, stderr = self.__mConverterLog, cwd=os.path.split(os.path.abspath(self.__mParams.psConverterExe))[0])
		if lRes.returncode != 0:
			self.__mLogger.critical('Error %i while running %s' % (lRes.returncode,str(lCmdLine)))
			raise Exception('Error %i while running %s' % (lRes.returncode,str(lCmdLine)))
		self.__mJobs.clear()


########################################
#
# default implementation of XRefResolverInteface this class to change the way xref are resolved
#
########################################
class FileSystemXRefResolver(XRefResolverInteface):
	def __init__(self, pBaseDir, pCacheFile, pLogger):
		XRefResolverInteface.__init__(self)
		
		self.__mLogger = pLogger
		# map : file name => array(relpath,filesize)
		self.__mFilePathMap = dict()
		self.__mBaseDir = pBaseDir
		
		lCptr = 0
		if (not pCacheFile is None) and os.path.isfile(pCacheFile) and (os.stat(pBaseDir).st_mtime < os.stat(pCacheFile).st_mtime):
			self.__mLogger.info('FileSystemXRefResolver load index of %s from cache %s ' % (pBaseDir,pCacheFile))

			with open(pCacheFile,'r') as f:
				self.__mFilePathMap = json.load(f)
			for f in self.__mFilePathMap:
				lCptr = lCptr + len(self.__mFilePathMap[f])
		else:
			
			self.__mLogger.info('FileSystemXRefResolver start indexing ' + pBaseDir)
			for (dirpath, dirnames, filenames) in os.walk(self.__mBaseDir):
				lCptrStart = lCptr
				for lFile in filenames:
					lFullFilePath = os.path.join(dirpath,lFile)
					lRelativePath = self.__normalizePath(lFullFilePath)
					lExt = os.path.splitext(lFullFilePath)[1].lower()
					if lExt in ['.catproduct','.jt','.catpart','.cgr','.model','.fbx','.obj','.plmxml']:
						lFileName = os.path.basename(lFullFilePath)
						if lFileName in self.__mFilePathMap:
							self.__mFilePathMap[lFileName].append((lRelativePath,os.path.getsize(lFullFilePath)))
							lCptr = lCptr + 1
						else:
							self.__mFilePathMap[lFileName] = [(lRelativePath,os.path.getsize(lFullFilePath))]
							lCptr = lCptr + 1
				if lCptr !=lCptrStart:
					self.__mLogger.debug('Index %d files from %s' % (lCptr-lCptrStart,dirpath)) 
				
			if not pCacheFile is None:
				if not os.path.isdir(os.path.dirname(pCacheFile)):
					os.makedirs(os.path.dirname(pCacheFile))
				with open(pCacheFile,'w') as f:
					json.dump(self.__mFilePathMap,f,sort_keys=True,indent='\t')
		self.__mLogger.info('FileSystemXRefResolver is ready with %d files ' % (lCptr) )
		
	def resolveXRef(self,pParentFilePath,pXRef):
		lXRef = pXRef.replace('\\','/')
		lFileName = os.path.basename(lXRef)
		if not lFileName in self.__mFilePathMap:
			self.__mLogger.warning("Fail to resolve xref " + lXRef + " unknown file")
			return None
		else:
			# get relative file path of parent
			lParentRelPath = self.__normalizePath(os.path.dirname(pParentFilePath))
			
			lRelPathList = self.__mFilePathMap[lFileName]
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
	
	def __normalizePath(self, pPath):
		lRes = os.path.relpath(pPath,self.__mBaseDir).replace('\\','/')
		if lRes == '.':
			lRes = ''
		return lRes



########################################
#
# this class is responsible to walk throught ps and convert it
# after conversion results will be uploaded to the generator
#
# /!\ this class SHOULD be used in a with ... as ... statement
#
########################################
class Converter3dji:
	def __init__(self, pParam, pCustomizer, pXRefSolver, pConverters, pLogger):
		if not isinstance(pParam, Converter3djiSettings):
			raise Exception('Invalid pParam')
		if not isinstance(pCustomizer, PsCustomizer):
			raise Exception('Invalid pCustomizer')
		if not isinstance(pXRefSolver, XRefResolverInteface):
			raise Exception('Invalid pXRefSolver')
		
		
		self.__mLogger = pLogger
		self.__mParam = pParam
		self.__mParam.checkValidity()
		
		if not self.__mParam.httpProxy:
			os.environ['no_proxy'] = '*'
			if 'http_proxy' in os.environ:
				del os.environ['http_proxy']
			if 'https_proxy' in os.environ:
				del os.environ['https_proxy']
		else:
			os.environ['http_proxy'] = self.__mParam.httpProxy
			os.environ['https_proxy'] = self.__mParam.httpProxy
		
		self.__mCustomizer = pCustomizer
		self.__mConverters = pConverters
		self.__mXRefSolver = pXRefSolver
		self.__mRemainingFilesToProcess = dict()
		self.__mAllProcessedFiles = set()
		self.__mPotentialRootFiles = set()
		self.__mServerAdapter = _ServerAdapter(pParam,pLogger)
		self.__mFilesToPush = dict()
	
	def __enter__(self):
		
		# take update lock
		if not self.__mServerAdapter.setProjectStatus('lockupdating'):
			self.__mLogger.critical('Fail to take update lock')
			raise Exception('Fail to take update lock')
		
		# index is cleared in constructor so user could call methods in whatever order he want
		if self.__mParam.clearConnectorIndex:
			self.__mLogger.info('clear es index')
			self.__mServerAdapter.removeOldDocuments()
		if self.__mParam.reprocessCacheErrors:
			self.__mLogger.info('clear cache errors')
			self.clearCacheErrors()
			
		return self
		
	def __exit__(self, exc_type, exc_value, traceback):
		if exc_type is None:
			self.__mServerAdapter.uploadBatch()
			self.__mServerAdapter.syncIndex()
			
			# release update lock
			if not self.__mServerAdapter.setProjectStatus('idle'):
				self.__mLogger.critical('Fail to release update lock')
				raise Exception('Fail to release update lock')
		else:
			if not self.__mServerAdapter.setProjectStatus('connectorerror'):
				self.__mLogger.critical('Fail to release update lock')
				raise Exception('Fail to release update lock')
	
	# this method will remove from cache all convresults that contains errors
	def __clearCacheErrors(self):
		for dir in os.listdir(self.mParam.cachefolder):
			lConvResultFile = os.path.join(self.mParam.cachefolder,dir,'convresult.json')
			if not os.path.isfile(lConvResultFile):
				continue
			lConvResult = self._loadJsonFile(lConvResultFile)
			if 'errors' in lConvResult and len(lConvResult['errors']) > 1:
				lInfoJson = self._loadJsonFile(os.path.join(self.mParam.cachefolder,dir,'info.json'))
				self.__mLogger.info('force reprocess of ' + lInfoJson['filepath'])
				os.remove(lConvResultFile)
				
			
		
	# use this method to upload documents
	# if pInput is a folder path, all json files will be added
	# if pInput is a file path, document will be loaded from file (ts will be updated based on file last modified date)
	# if pInput could be a dict representing the document
	def addDocument(self, pInput, ts=None):
		if isinstance(pInput,dict):
			if ts is not None:
				pInput['ts'] = ts
			else:
				pInput['ts'] = round(datetime.datetime.now().timestamp())
			self.__mServerAdapter.addDocument(pInput)
		elif isinstance(pInput,list):
			for j in pInput:
				self.addDocument(j,ts)
		elif os.path.isdir(pInput):
			for file in os.listdir(pInput):
				self.addDocument(os.path.join(pInput,file),ts)
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
	def addClientScript(self, pFilePath, **kwargs):
		with open(pFilePath,'r',encoding='utf-8') as f:
			lScript = f.read()
		scriptdoc = {
				'id':'com.3djuump:scripts',
				'type':'projectdocument',
				'version' : '9.1',
				'subtype':'scripts',
				'scriptbase64' : base64.b64encode(lScript.encode('utf-8')).decode('ascii'),
				'taskscripts' : {},
				'ts': self._getFileTs(pFilePath)
		}
		for tasktype,filepath in kwargs.items():
			with open(filepath,'r',encoding='utf8') as f:
				taskscript = f.read()
			scriptdoc['taskscripts'][tasktype] = base64.b64encode(taskscript.encode('utf-8')).decode('ascii')
			scriptdoc['ts'] = max(scriptdoc['ts'],self._getFileTs(filepath))
		self.__mServerAdapter.addDocument(scriptdoc)
	
	def getDefaultBuildParameters(self):
		lServerCap = self.__mServerAdapter.getServerCapabilities()
		lRamCount = lServerCap['ram_quantity_bytes'] / (1024*1024)
		lRamLimit = 2048
		lCpuCount = max(1,min(lServerCap['cpu_count'], math.floor( (lRamCount * 0.8) / lRamLimit )))
		
		return {
			"id" : "com.3djuump:buildparameters",
			"ts":0,
			"type" : "projectdocument",
			"version" : "9.1",
			"subtype" : "buildparameters",
			"buildparameters" : {
				"rootstructuredocid" : "...",
				"applicableconfigurations":None,
				"tags":[],
				"sourcers" : {
					"converter3dji_pushedfiles":{
						"baseurl":'$LOCAL$',
						"type":"FileSystemSourcer"
					}
				},
				"defaultgeometrysettings" : {
					"sourcer" : "defaultsourcer",
					"connexitythreshold" : 2,
					"backfaceculling":"ccw",
					"visibilityvoxelizationstep" : 175,
					"dynamiclowdefvoxelizationstep" : 50,
					"maxvoxelcount" : 200000,
					"allowstaticlowdef" : True,
					"allowdynamiclowdef" : True,
					"minobjsizeforstaticlowdef" : 300,
					"minobjsizefordynamiclowdef" : 175,
					"minheuristicfordynamiclowdef" : 0.6,
					"minheuristictoprioritizedynamiclowdef" : 0.98,
					"subpartlevel":"body",
					"etag":None
				},
				"xformtolerance" : {
					"translation" : 0.01,
					"rotation" : 0.001
				},
				"lowdeftrlcount" : 1000000,
				"buildcomment" : "Build comment",
				"visiblersets" : [10000, 20000],
				"modelaabblimit" : {
					"xmin" : -3.3e38,
					"xmax" : 3.3e38,
					"ymin" : -3.3e38,
					"ymax" : 3.3e38,
					"zmin" : -3.3e38,
					"zmax" : 3.3e38
				},
				"workertimeoutsec":900,
				"workermemorylimitMB":lRamLimit,
				"workercount":lCpuCount
			}
		}
	
	# call this method to process product structure
	# this method will return list of generated root ids 
	def convert(self,pRootFiles, pGenerateTopNode = True):
		self.__mLogger.info('Start processing')
		lRootIds = []
		self.mFilesToPush = dict()
		
		lRootFiles = pRootFiles
		if not isinstance(lRootFiles,list):
			if not isinstance(lRootFiles,str):
				self.__mLogger.critical('input should be list or str')
				raise Exception('input should be list or str')
			lRootFiles = [lRootFiles]
		lGenerateTopNode = pGenerateTopNode and len(lRootFiles) > 1
		
		if lGenerateTopNode:
			lRootIds.append('root')
			lRootDoc = {
				'id':'root',
				'type':'structure',
				'partmdid' : 'partmd_root',
				'children' : {}
				}
			for r in lRootFiles:
				(lChildId,_,_,_) = self._computeFileInfo(r)
				lRootDoc['children']['root_' + lChildId] = { 'ref':lChildId }
			self.__mServerAdapter.addDocument(lRootDoc)
		self.__mLogger.info('Convert %i root file%s, %s top node' % (len(lRootFiles), 's' if len(lRootFiles) > 1 else '', 'with' if lGenerateTopNode else 'without'))
		
		self.mRemainingFilesToProcess = dict()
		for r in lRootFiles:
			self.__mRemainingFilesToProcess[r] = 0.
			self.__mPotentialRootFiles.add(r)
			if not lGenerateTopNode:
				(lRootId,_,_,_) = self._computeFileInfo(r)
				lRootIds.append(lRootId)
		

		self.__mAllProcessedFiles = set()
		while len(self.__mRemainingFilesToProcess) > 0:
			lToConvert = []
			# iterate over self.mRemainingFilesToProcess until it is not empty files
			# could have been added during loop if an entry was already in the cache
			lAnalyzedFileCounter = 0
			while len(self.__mRemainingFilesToProcess) > 0:
				lCurrentBatch = []
				for k in self.__mRemainingFilesToProcess:
					lCurrentBatch.append((k,self.__mRemainingFilesToProcess[k]))
				lCurrentBatch.sort(key=lambda x:-x[1])
				lAnalyzedFileCounter = lAnalyzedFileCounter + len(lCurrentBatch)
				self.__mRemainingFilesToProcess = dict()
				for (lBatchEntry,lWeight) in lCurrentBatch:
					if not os.path.isfile(lBatchEntry):
						self.__mLogger.warning('Missing file %s' % (lBatchEntry))
						continue
					lEtag = self._getFileTs(lBatchEntry)
					(lFileHash,lCacheFolder,lConvResultFile,lInfoJsonFile) = self._computeFileInfo(lBatchEntry)
					if not os.path.isdir(lCacheFolder):
						os.makedirs(lCacheFolder)
					lInfoJson = self._loadJsonFile(lInfoJsonFile)
					if( 	(not 'etag' in lInfoJson) or 
							(lInfoJson['etag'] != lEtag) or 
							(not 'filepath' in lInfoJson) or 
							(lInfoJson['filepath'] != lBatchEntry) or
							(not os.path.isfile(lConvResultFile))
						):
						# clear cache
						for fc in os.listdir(lCacheFolder):
							file_path = os.path.join(lCacheFolder, fc)
							if os.path.isfile(file_path):
								os.unlink(file_path)
							
						# need to reprocess file
						lToConvert.append({**{
								'file':lBatchEntry,
								'rootid':lFileHash,
								'rubfolder':os.path.abspath(lCacheFolder),
								'convresult':os.path.abspath(lConvResultFile),
								'logfile':os.path.abspath(os.path.join(os.path.dirname(lConvResultFile),'log.txt'))
							},**self.__mCustomizer.computeExtractSettings(lBatchEntry)})
						if self.__mParam.copyBeforeLoad is not None:
							lToConvert[-1]['copybeforeload'] = os.path.abspath(self.__mParam.cacheFolder)
					else:
						lConvResult = self._loadJsonFile(lConvResultFile)
						if self.__mParam.reprocessDocFromCache :
							self._callPsCustomizer(lConvResult,lFileHash,lBatchEntry,lConvResult['infos']['ts'],True)
							with open(lConvResultFile,'w') as of:
								json.dump(lConvResult,of)
						self._analyzeconvresult(lBatchEntry,lFileHash,lCacheFolder,lConvResult)
					
			self.__mLogger.info('Analyze %i files, %i are outdated ' % (lAnalyzedFileCounter,len(lToConvert)))
			
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
			for c in lToConvert:
				(lFileHash,lCacheFolder,lConvResultFile,lInfoJsonFile) = self._computeFileInfo(c['file'])
				if not os.path.isfile(lConvResultFile):
					self.__mLogger.error('Fail to retrieve convert result ''%s'' ''%s''' % (c['file'], lFileHash))
					continue
				lConvResult = self._loadJsonFile(lConvResultFile)
				lGeomDocs = {doc['id']:doc for doc in lConvResult['docs'] if doc['type'] == 'geometry'}
				lFileEtag = os.stat(lConvResultFile).st_mtime
				if lFileEtag is not None:
					lFileEtag = str(lFileEtag)
					
				# generate geometry documents if needed
				lGeometryDocs = []
				for lDoc in lConvResult['docs']:
					if lDoc['type'] == 'structure' and 'geometry' in lDoc and lDoc['geometry'] not in lGeomDocs:
						lGeometryDoc = {
							'id':lDoc['geometry'],
							'type':'geometry',
							'geometrysettings':{
								'path': lDoc['geometry'] + '.rub',
								'loginfo' : 'geometry of ' + lFileHash,
								'sourcer':'converter3dji_pushedfiles',
								'etag':lFileEtag
							}
						}
						lGeometryDocs.append(lGeometryDoc)
				lConvResult['docs'] = lConvResult['docs'] + lGeometryDocs
				
				
				self._callPsCustomizer(lConvResult,lFileHash,c['file'],lConvResult['infos']['ts'])
				
				# re-save convresult it might have been modified by ps converter
				with open(lConvResultFile,'w') as of:
					json.dump(lConvResult,of)

				self._analyzeconvresult(c['file'],lFileHash,lCacheFolder,lConvResult)
				
				lInfoJson = {
						'etag': self._getFileTs(c['file']),
						'filepath' : c['file'],
						'rootid':lFileHash
					}
				with open(lInfoJsonFile,'w') as f:
					json.dump(lInfoJson,f,sort_keys=True)
				
		
		self.__mServerAdapter.pushGeometryFiles(self.mFilesToPush)
		
		return lRootIds

	
	def _callPsCustomizer(self, pConvResult, pRootId, pSourceFilePath, pTs, pIncrementTs=False):
		lIndexedDocs = dict()
		for d in pConvResult['docs']:
			lIndexedDocs[d['id']] = d
		
		if not pRootId in lIndexedDocs:
			if not 'errors' in pConvResult:
				pConvResult['errors'] = []
			pConvResult['errors'].append('root document is missing from converter result')
			return
		
		self.__mCustomizer.processConvResult(lIndexedDocs,pRootId,pSourceFilePath)
		
		if not pRootId in lIndexedDocs:
			if not 'errors' in pConvResult:
				pConvResult['errors'] = []
			pConvResult['errors'].append('root document was removed by ps customizer')
			return
			
		pConvResult['docs']=[]
		for k in lIndexedDocs:
			if not 'ts' in lIndexedDocs[k]:
				lIndexedDocs[k]['ts'] = pTs
			if pIncrementTs:
				lIndexedDocs[k]['ts'] = lIndexedDocs[k]['ts'] + 1
			pConvResult['docs'].append(lIndexedDocs[k])
	
	def _loadJsonFile(self, pFileName):
		try:
			with open(pFileName,'r',encoding='utf-8') as f:
				return json.load(f)
		except:
			return {}
	
	def _getFileTs(self, pFileName):
		return round(os.path.getmtime(pFileName))
	
	def _computeFileInfo(self,pFileName):
		m = hashlib.sha256()
		m.update(pFileName.encode('utf8'))
		lHash = base64.b64encode(m.digest()).decode('ascii')
		lHash = lHash.replace('/','_')
		
		lCacheFolder = os.path.split(pFileName)[1] + ' ' + lHash
		return ('hash_' + lHash, 
			os.path.join(self.__mParam.cacheFolder,lCacheFolder), 
			os.path.join(self.__mParam.cacheFolder,lCacheFolder,'convresult.json'),
			os.path.join(self.__mParam.cacheFolder,lCacheFolder,'info.json'))
		
	def _analyzeconvresult(self, pParentFilePath,pParentHash, pCacheFolder, pConvResult):
		if 'errors' in pConvResult:
			for e in pConvResult['errors']:
				self.__mLogger.error('error %s => %s' % (pParentHash,pConvResult['errors']))
		if 'warnings' in pConvResult:
			for w in pConvResult['warnings']:
				self.__mLogger.warning('warnings %s => %s' % (pParentHash,pConvResult['warnings']))
		
		# look for xrefs and rub files
		lXRefs = dict()
		for lDoc in pConvResult['docs']:
			lFinalDoc = lDoc
			
			if lDoc['type'] == 'structure' and 'children' in lDoc:
				lFinalDoc = copy.deepcopy(lDoc)
				lFinalDoc['children'] = {}
				for c in lDoc['children']:
					lChild = copy.deepcopy(lDoc['children'][c])
					if 'psconverter:xref' in lChild:
						lXRef = self.__mXRefSolver.resolveXRef(pParentFilePath,lChild['psconverter:xref'])
						del lChild['psconverter:xref']
						if lXRef is None:
							continue
						(lChild['ref'],_,_,_) = self._computeFileInfo(lXRef[0])
						lXRefs[lXRef[0]] = lXRef[1]
					lLinkId = c
					if 'psconverter:xrefmetadata' in lChild:
						lLinkMdDoc = {}
						# create a link metadata, a better solution would be to set those metadata on xref root but here we have no knowlage of it and we might need to ensure that this extra metadata block is the same each time this xref is used
						# for now don't bother to add metadata on an existing document
						if not lChild['hasmetadata']:
							# create a new one
							lChild['hasmetadata'] = True
							lLinkId = lDoc['id'] + '_link_' + c
							lLinkDoc = {
								'id' : lLinkId,
								'type':'linkmetadata',
								'metadata': lChild['psconverter:xrefmetadata'],
								'ts':lDoc['ts']
							}
							self.__mServerAdapter.addDocument(lLinkDoc)
						
						del lChild['psconverter:xrefmetadata']
					lFinalDoc['children'][lLinkId] = lChild
			elif lDoc['type'] == 'geometry' and lDoc['geometrysettings']['sourcer'] == 'converter3dji_pushedfiles':
				lFileName = lDoc['geometrysettings']['path']
				if lFileName in self.mFilesToPush:
					raise Exception('got a geometry file name conflict')
				self.mFilesToPush[lFileName] = os.path.join(pCacheFolder,lFileName)
			
			self.__mServerAdapter.addDocument(lFinalDoc)
		lXRefsSet = set(lXRefs.keys())
		for k in ( lXRefsSet - self.__mAllProcessedFiles):
			self.__mRemainingFilesToProcess[k] = lXRefs[k]
		self.__mAllProcessedFiles = self.__mAllProcessedFiles | lXRefsSet
		self.__mPotentialRootFiles = self.__mPotentialRootFiles - lXRefsSet

########################################
#
# internal class used by PsConverter to interact with es index
#
########################################
class _ServerAdapter:
	def __init__(self, pParams, pLogger):
		self.__mLogger = pLogger
		self.__mCurrentEsBatch = io.BytesIO()
		self.__mParam = pParams
		self.__mUrlBase = self.__mParam.proxyApiUrl + '/elastic/' + self.__mParam.projectId + '_connector'
		
		# extract credentials
		lCredentialsMatch = re.match(r'^https:\/\/(.+?):(.+?)@.*\/proxy$',self.__mParam.proxyApiUrl)
		if lCredentialsMatch is None or len(lCredentialsMatch.groups()) != 2:
			raise Exception('Invalid proxy url, fail to extract credentials')
		self.__mProxyApiKey = base64.b64encode((urllib.parse.quote_plus(lCredentialsMatch.groups()[0]) + ':' + urllib.parse.quote_plus(lCredentialsMatch.groups()[1])).encode('utf-8'))
		
		self.__mPool = requests.Session()
		if not self.__mParam.verifySSL:
			self.__mPool.verify = False
			requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
	
	def setProjectStatus(self, pStatus):
		lUrl = self.__mParam.proxyApiUrl + '/api/manage/generator/project/' + self.__mParam.projectId + '/status?projectstatus=' + pStatus
		lResponse = self.__mPool.put(lUrl,headers={'x-infinite-apikey':self.__mProxyApiKey})
		if lResponse.status_code != 200:
			self.__mLogger.error(lResponse.text)
			return False
		return True
		
	def getServerCapabilities(self):
		lUrl = self.__mParam.proxyApiUrl + '/api/manage/generator/getcapabilities'
		lResponse = self.__mPool.get(lUrl,headers={'x-infinite-apikey':self.__mProxyApiKey})
		if lResponse.status_code != 200:
			self.__mLogger.critical('Fail to retrieve server capabilities')
			raise Exception('Fail to retrieve server capabilities')
		return lResponse.json();
	
	def addDocument(self, pDoc):
		lToAppend = b'{"index":{"_id":"' + pDoc['id'].encode('utf8') + b'"}}\n'
		# our script has added ts on all documents remove those that should not be here
		if pDoc['type'] in ['structure','geometry','annotation'] and 'ts' in pDoc:
			del pDoc['ts']
		lToAppend = lToAppend + json.dumps(pDoc).encode('utf8') + b'\n'
		
		if self.__mCurrentEsBatch.getbuffer().nbytes + len(lToAppend) > 80*1024*1024:
			self.uploadBatch()
		self.__mCurrentEsBatch.write(lToAppend)
	
	def uploadBatch(self):
		if self.__mCurrentEsBatch.getbuffer().nbytes == 0:
			return
		lToSend = self.__mCurrentEsBatch.getvalue()
		lResponse = self.__mPool.post(self.__mUrlBase + '/_doc/_bulk', data=lToSend, headers={"Content-Type": "application/x-ndjson"})
		if lResponse.status_code != 200:
			with open(self.__mParam.cachefolder + '/eserror.log','w') as f:
				f.write('Invalid return code for _bulk\n' + str(lResponse.status_code) + '\n' + lResponse.reason + '\n' + str(lResponse.text)+ '\n' + lToSend.decode('utf8'))
			self.__mLogger.critical('Es error, please check eserror.log')
			raise Exception('Es error, please check eserror.log');
		if lResponse.json()['errors']:
			with open(self.__mParam.cachefolder + '/eserror.log','w') as f:
				f.write('Insertion error\n')
				for i in lResponse.json()['items']:
					if 'index' in i and 'error' in i['index']:
						f.write(json.dumps(i['index']) + '\n')
			with open(self.__mParam.cachefolder + '/lastesbatch.txt','wb') as f:
				f.write(lToSend)
			self.__mLogger.critical('Es error, please check eserror.log')
			raise Exception('Es error, please check eserror.log')
		self.__mLogger.info('Insert %i docs in the index'%(len(lResponse.json()['items'])))
		self.__mCurrentEsBatch = io.BytesIO()
	
	
	def syncIndex(self):
		lResponse = self.__mPool.post(self.__mUrlBase + '/_flush')
		if(lResponse.status_code != 200):
			self.__mLogger.critical('Invalid return code for _flush ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
			raise Exception('Invalid return code for _flush ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
	
	def removeOldDocuments(self):
		lQuery = {
			'query':{
				'terms':{
					'type':['structure','partmetadata','linkmetadata','annotation','geometry','instancemetadata','projectdocument','conf']
				}
			}
		}
		lResponse = self.__mPool.post(self.__mUrlBase + '/_delete_by_query', json=lQuery, headers={"Content-Type": "application/json"})
		if(lResponse.status_code != 200):
			self.__mLogger.critical('Invalid return code for _delete_by_query ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
			raise Exception('Invalid return code for _delete_by_query ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
		lJson = lResponse.json()
		self.__mLogger.info('Remove %i documents from the index' % (lJson["deleted"]))
	
	def pushGeometryFiles(self, pFiles):
		lUrl = self.__mParam.proxyApiUrl + '/api/manage/generator/project/' + self.__mParam.projectId + '/pushfile'
		lResponse = self.__mPool.post(lUrl,data=json.dumps(list(pFiles.keys())), headers={'x-infinite-apikey':self.__mProxyApiKey,"Content-Type": "application/json"})
		if lResponse.status_code != 200:
			self.__mLogger.critical('Invalid return code for POST /pushfile ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
			raise Exception('Invalid return code for POST /pushfile ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
		lResponseJson = lResponse.json()
		
		lFilesToSend = []
		lSendFiles = 0
		for k in lResponseJson:
			if lResponseJson[k]:
				continue
			# prepare http mutltipart name=geometry filename=...
			lFilesToSend.append(('geometry',(k,open(pFiles[k],'rb'),'application/octet-stream')))
			
			if len(lFilesToSend) > 2047:
				self.__pushFiles(lFilesToSend)
				lSendFiles = lSendFiles + len(lFilesToSend)
				lFilesToSend.clear()
				
		if len(lFilesToSend) > 0:
			self.__pushFiles(lFilesToSend)
			lSendFiles = lSendFiles + len(lFilesToSend)
			lFilesToSend.clear()
			
		self.__mLogger.info('Push %d rub files, %d are up to date'%(lSendFiles,len(pFiles)-lSendFiles))
		
	def __pushFiles(self,pFiles):
		if len(pFiles) == 0:
			return
		lUrl = self.__mParam.proxyApiUrl + '/api/manage/generator/project/' + self.__mParam.projectId + '/pushfile'
		lResponse = self.__mPool.put(lUrl,files=pFiles, headers={'x-infinite-apikey':self.__mProxyApiKey})
		if lResponse.status_code != 200:
			self.__mLogger.critical('Invalid return code for PUT /pushfile ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
			raise Exception('Invalid return code for PUT /pushfile ' + str(lResponse.status_code) + ' ' + lResponse.reason + ' ' + str(lResponse.text))
		
