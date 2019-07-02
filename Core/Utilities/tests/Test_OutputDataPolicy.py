"""Tests for OutputDataPolicy."""

from itertools import product
from parameterized import parameterized

WORKFLOW_XML = """
<?xml version="1.0" encoding="UTF-8" ?><Workflow>
<origin></origin>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<version>0.0</version>
<type></type>
<name>ILD-Opt_sim_1000_6f_WWS_s5_v02_20190619_1</name>
<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified type"><value><![CDATA[%(JOB_TYPE)s]]></value></Parameter>
<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>
<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>
<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>
<Parameter name="Site" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Site Requirement"><value><![CDATA[ANY]]></value></Parameter>
<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>
<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>
<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>
<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>
<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>
<Parameter name="Platform" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Platform ( Operating System )"><value><![CDATA[x86_64-slc5-gcc43-opt]]></value></Parameter>
<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>
<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>
<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>
<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>
<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>
<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>
<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>
<Parameter name="SoftwarePackages" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="ILC Software Packages to be installed"><value><![CDATA[ildconfig.v02-00-02;ddsim.ILCSoft-02-00-02_gcc49]]></value></Parameter>
<Parameter name="ILDConfigPackage" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="ILDConfig package"><value><![CDATA[ILDConfigv02-00-02]]></value></Parameter>
<Parameter name="BannedSites" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Site excluded by user"><value><![CDATA[]]></value></Parameter>
<Parameter name="Energy" type="float" linked_module="" linked_parameter="" in="True" out="False" description="Energy used"><value><![CDATA[1000.0]]></value></Parameter>
<Parameter name="TotalSteps" type="String" linked_module="" linked_parameter="" in="True" out="False" description="Total number of steps"><value><![CDATA[1]]></value></Parameter>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile]]></body>
<origin></origin>
<description><![CDATA[Uploads the output log files]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>UploadLogFile</type>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.ComputeOutputDataList import ComputeOutputDataList
]]></body>
<origin></origin>
<description><![CDATA[Compute the output data list to be treated by the last finalization]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>ComputeOutputDataList</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.DDSimAnalysis import DDSimAnalysis
]]></body>
<origin></origin>
<description><![CDATA[Module to run DDSim]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>DDSimAnalysis</type>
<Parameter name="randomSeed" type="int" linked_module="" linked_parameter="" in="False" out="False" description="Random seed for the generator"><value><![CDATA[0]]></value></Parameter>
<Parameter name="detectorModel" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Detecor model for simulation"><value><![CDATA[]]></value></Parameter>
<Parameter name="startFrom" type="int" linked_module="" linked_parameter="" in="False" out="False" description="From where DDSim starts to read the input file"><value><![CDATA[0]]></value></Parameter>
<Parameter name="debug" type="bool" linked_module="" linked_parameter="" in="False" out="False" description="debug mode"><value><![CDATA[False]]></value></Parameter>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.ILDRegisterOutputData import ILDRegisterOutputData]]></body>
<origin></origin>
<description><![CDATA[Module to add in the metadata catalog the relevant info about the files]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>ILDRegisterOutputData</type>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData]]></body>
<origin></origin>
<description><![CDATA[Uploads the output data]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>UploadOutputData</type>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.ReportErrors import ReportErrors]]></body>
<origin></origin>
<description><![CDATA[Reports errors at the end]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>ReportErrors</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest]]></body>
<origin></origin>
<description><![CDATA[Sends any failover requests]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>FailoverRequest</type>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>
</ModuleDefinition>
<StepDefinition>
<origin></origin>
<version>0.0</version>
<type>ddsim_step_1</type>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<Parameter name="applicationName" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Application Name"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationVersion" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Application Version"><value><![CDATA[]]></value></Parameter>
<Parameter name="SteeringFile" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Steering File"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Log File"><value><![CDATA[]]></value></Parameter>
<Parameter name="ExtraCLIArguments" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Extra CLI arguments"><value><![CDATA[]]></value></Parameter>
<Parameter name="InputFile" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Input File"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputFile" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Output File"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputPath" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output File path on the grid"><value><![CDATA[]]></value></Parameter>
<Parameter name="outputPathREC" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output REC File path on the grid"><value><![CDATA[]]></value></Parameter>
<Parameter name="outputPathDST" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output DST File path on the grid"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputSE" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output File storage element"><value><![CDATA[]]></value></Parameter>
<Parameter name="listoutput" type="list" linked_module="" linked_parameter="" in="False" out="False" description="list of output file name"><value><![CDATA[[]]]></value></Parameter>
<Parameter name="ForgetInput" type="boolean" linked_module="" linked_parameter="" in="False" out="False" description="Do not overwrite input steering"><value><![CDATA[False]]></value></Parameter>
<ModuleInstance>
<type>DDSimAnalysis</type>
<name>ddsim_step_1</name>
<descr_short></descr_short>
<Parameter name="randomSeed" type="int" linked_module="" linked_parameter="" in="False" out="False" description="Random seed for the generator"><value><![CDATA[-1]]></value></Parameter>
<Parameter name="detectorModel" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Detecor model for simulation"><value><![CDATA[ILD_s5_v02]]></value></Parameter>
<Parameter name="startFrom" type="int" linked_module="" linked_parameter="" in="False" out="False" description="From where DDSim starts to read the input file"><value><![CDATA[0]]></value></Parameter>
<Parameter name="debug" type="bool" linked_module="" linked_parameter="" in="False" out="False" description="debug mode"><value><![CDATA[False]]></value></Parameter>
</ModuleInstance>
<ModuleInstance>
<type>ComputeOutputDataList</type>
<name>ddsim_step_1</name>
<descr_short></descr_short>
</ModuleInstance>
</StepDefinition>
<StepDefinition>
<origin></origin>
<version>0.0</version>
<type>Job_Finalization</type>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<ModuleInstance>
<type>UploadOutputData</type>
<name>dataUpload</name>
<descr_short></descr_short>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>
</ModuleInstance>
<ModuleInstance>
<type>ILDRegisterOutputData</type>
<name>ILDRegisterOutputData</name>
<descr_short></descr_short>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>
</ModuleInstance>
<ModuleInstance>
<type>UploadLogFile</type>
<name>logUpload</name>
<descr_short></descr_short>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>
</ModuleInstance>
<ModuleInstance>
<type>FailoverRequest</type>
<name>failoverRequest</name>
<descr_short></descr_short>
<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>
</ModuleInstance>
<ModuleInstance>
<type>ReportErrors</type>
<name>reportErrors</name>
<descr_short></descr_short>
</ModuleInstance>
</StepDefinition>
<StepInstance>
<type>ddsim_step_1</type>
<name>ddsim_step_1</name>
<descr_short></descr_short>
<Parameter name="applicationName" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Application Name"><value><![CDATA[ddsim]]></value></Parameter>
<Parameter name="applicationVersion" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Application Version"><value><![CDATA[ILCSoft-02-00-02_gcc49]]></value></Parameter>
<Parameter name="SteeringFile" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Steering File"><value><![CDATA[ddsim_steer.py]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Log File"><value><![CDATA[ddsim_ILCSoft-02-00-02_gcc49_@{STEP_ID}.log]]></value></Parameter>
<Parameter name="ExtraCLIArguments" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Extra CLI arguments"><value><![CDATA[%%20--vertexSigma%%200.0%%200.0%%200.1468%%200.0%%20--vertexOffset%%200.0%%200.0%%200.0%%200.0%%20]]></value></Parameter>
<Parameter name="InputFile" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Input File"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputFile" type="string" linked_module="" linked_parameter="" in="False" out="False" description="Output File"><value><![CDATA[%(OUTPUT_FILE)s]]></value></Parameter>
<Parameter name="OutputPath" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output File path on the grid"><value><![CDATA[/ilc/prod/ilc/mc-opt-3/ild/sim/1000-B1b_ws/6f_WWS/ILD_s5_v02/v02-00-02/]]></value></Parameter>
<Parameter name="outputPathREC" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output REC File path on the grid"><value><![CDATA[]]></value></Parameter>
<Parameter name="outputPathDST" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output DST File path on the grid"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputSE" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Output File storage element"><value><![CDATA[DESY-SRM]]></value></Parameter>
<Parameter name="listoutput" type="list" linked_module="" linked_parameter="" in="False" out="False" description="list of output file name"><value><![CDATA[[{'outputPath': '@{OutputPath}', 'outputDataSE': '@{OutputSE}', 'outputFile': '@{OutputFile}'}]]]></value></Parameter>
<Parameter name="ForgetInput" type="boolean" linked_module="" linked_parameter="" in="False" out="False" description="Do not overwrite input steering"><value><![CDATA[False]]></value></Parameter>
</StepInstance>
<StepInstance>
<type>Job_Finalization</type>
<name>finalization</name>
<descr_short></descr_short>
</StepInstance>
</Workflow>
"""  # noqa: E501


def nameTests(testcase_func, param_num, param):
  """Construct the name for the tests from the parameter tuple."""
  return '%s_%s_%s_%s' % (testcase_func.__name__, param[0][1], param[0][0].__class__.__name__, param_num)


@parameterized.expand(product(('/vo/input/file1', ['/vo/input/file1'], None),
                              ('MCSimulation', 'MCSimulation_ILD'),
                              ), testcase_func_name=nameTests)
def test_execute(inputData, jobType):
  """Test: OutputDataPolicyExecute"""
  outputFile = 'sv02-00-02.mILD_s5_v02.E1000-B1b_ws_sim.slcio'
  from ILCDIRAC.Core.Utilities.OutputDataPolicy import OutputDataPolicy
  paramDict = dict()
  paramDict['Job'] = WORKFLOW_XML.strip() % {'JOB_TYPE': jobType, 'OUTPUT_FILE': outputFile}
  paramDict['TransformationID'] = 12345
  paramDict['TaskID'] = 555
  paramDict['InputData'] = inputData
  odp = OutputDataPolicy(paramDict)
  result = odp.execute()
  assert result['OK']
  assert len(result['Value']['ProductionOutputData']) == 1
  assert result['Value']['ProductionOutputData'][0].endswith('12345_555.slcio')
