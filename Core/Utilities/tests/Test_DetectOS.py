"""
Unit tests for the DetectOS module
"""

import unittest
from mock import patch, mock_open, MagicMock as Mock
from ILCDIRAC.Core.Utilities.DetectOS import NativeMachine
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved

__RCSID__ = "$Id$"
MODULE_NAME = 'ILCDIRAC.Core.Utilities.DetectOS'

class TestNativeMachine( unittest.TestCase ):
  """ Tests the NativeMachine class
  """

  #pylint: disable=W0212
  def test_constructor( self ):
    with patch('%s.platform.uname' % MODULE_NAME, new=Mock(return_value=['TestOS', 'hostname', 'kernel version', 'date', 'x86_64'])):
      mach_unix = NativeMachine()
    mach_win32 = get_win32_machine()
    mach_win64 = get_win64_machine()
    with patch('%s.platform.uname' % MODULE_NAME, new=Mock(return_value=['Linux', 'hostname', 'kernel version', 'date', 'x86_64'])):
      mach_linux_64bit = NativeMachine()
    with patch('%s.platform.uname' % MODULE_NAME, new=Mock(return_value=['Linux', 'hostname', 'kernel version', 'date', 'i686'])):
      mach_linux_32bit = NativeMachine()
    os_mock = Mock()
    os_mock.read.return_value = 'powerpc1'
    with patch('%s.platform.uname' % MODULE_NAME, new=Mock(return_value=['Darwin', 'hostname', 'kernel version', 'date', 'i686'])), \
         patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=os_mock)):
      mach_darwin_PowerPC = NativeMachine()

    assertEqualsImproved( mach_unix._sysinfo, ['TestOS', 'hostname', 'kernel version', 'date', 'x86_64'], self )
    self.assertIsNone( mach_unix._arch )
    assertEqualsImproved( mach_unix._ostype, 'TestOS', self )
    assertEqualsImproved( mach_unix._machine, 'x86_64', self )

    assertEqualsImproved( mach_win32._sysinfo, [ 'mysystem' ], self )
    assertEqualsImproved( mach_win32._arch, '32', self  )
    assertEqualsImproved( mach_win32._ostype, 'Windows', self  )
    assertEqualsImproved( mach_win32._machine, 'i686', self )

    self.assertIsNone( mach_win64._sysinfo )
    assertEqualsImproved( mach_win64._arch, '64', self  )
    assertEqualsImproved( mach_win64._ostype, 'Windows', self  )
    assertEqualsImproved( mach_win64._machine, 'x86_64', self )

    assertEqualsImproved( mach_linux_64bit._sysinfo, ['Linux', 'hostname', 'kernel version', 'date', 'x86_64'], self )
    assertEqualsImproved( mach_linux_64bit._arch, '64', self  )
    assertEqualsImproved( mach_linux_64bit._ostype, 'Linux', self )
    assertEqualsImproved( mach_linux_64bit._machine, 'x86_64', self )

    assertEqualsImproved( mach_linux_32bit._sysinfo, ['Linux', 'hostname', 'kernel version', 'date', 'i686'], self )
    assertEqualsImproved( mach_linux_32bit._arch, '32', self  )
    assertEqualsImproved( mach_linux_32bit._ostype, 'Linux', self )
    assertEqualsImproved( mach_linux_32bit._machine, 'i686', self )

    assertEqualsImproved( mach_darwin_PowerPC._sysinfo, ['Darwin', 'hostname', 'kernel version', 'date', 'i686'], self )
    assertEqualsImproved( mach_darwin_PowerPC._arch, 'ppc', self  )
    assertEqualsImproved( mach_darwin_PowerPC._ostype, 'Darwin', self )
    assertEqualsImproved( mach_darwin_PowerPC._machine, 'i686', self )

  def test_osflavor_win( self ):
    nm = get_win32_machine()
    nm._sysinfo = ('Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel')
    result = nm.OSFlavour()
    assertEqualsImproved( result, '2008ServerR2', self )
    assertEqualsImproved( nm._osflavor, '2008ServerR2', self )

  def test_osflavor_sun( self ):
    nm = get_win32_machine()
    nm._ostype = 'SunOS'
    result = nm.OSFlavour()
    assertEqualsImproved( result, 'sun', self )
    assertEqualsImproved( nm._osflavor, 'sun', self )
    assertEqualsImproved( nm._osversion, '4.x', self )

  def test_osflavor_darwin_1( self ):
    nm = get_win32_machine()
    nm._ostype = 'Darwin'
    nm._sysinfo = ('Darwin', 'hubert.local', '7.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    result = nm.OSFlavour()
    assertEqualsImproved( result, 'Panther', self )
    assertEqualsImproved( nm._osflavor, 'Panther', self )
    assertEqualsImproved( nm._osversion, '10.3.4', self )

  def test_osflavor_darwin_2( self ):
    nm = get_win32_machine()
    nm._ostype = 'Darwin'
    nm._sysinfo = ('Darwin', 'hubert.local', '8.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    result = nm.OSFlavour()
    assertEqualsImproved( result, 'Tiger', self )
    assertEqualsImproved( nm._osflavor, 'Tiger', self )
    assertEqualsImproved( nm._osversion, '10.4.4', self )

  def test_osflavor_darwin_3( self ):
    nm = get_win32_machine()
    nm._ostype = 'Darwin'
    nm._sysinfo = ('Darwin', 'hubert.local', '9.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    result = nm.OSFlavour()
    assertEqualsImproved( result, 'Leopard', self )
    assertEqualsImproved( nm._osflavor, 'Leopard', self )
    assertEqualsImproved( nm._osversion, '10.5.4', self )

  def test_osflavor_darwin_4( self ):
    nm = get_win32_machine()
    nm._ostype = 'Darwin'
    nm._sysinfo = ('Darwin', 'hubert.local', '10.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    result = nm.OSFlavour()
    assertEqualsImproved( result, 'Snow Leopard', self )
    assertEqualsImproved( nm._osflavor, 'Snow Leopard', self )
    assertEqualsImproved( nm._osversion, '10.6.4', self )

  def test_osflavor_darwin_unknown( self ):
    nm = get_win32_machine()
    nm._ostype = 'Darwin'
    nm._sysinfo = ('Darwin', 'hubert.local', '11.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    result = nm.OSFlavour()
    assertEqualsImproved( result, None, self )
    assertEqualsImproved( nm._osflavor, None, self )
    assertEqualsImproved( nm._osversion, '10.7.4', self )

  def test_osflavor_misc( self ):
    nm = get_win32_machine()
    nm._ostype = 'misc'
    self.assertIsNone( nm.OSFlavour() )
    self.assertIsNone( nm._osflavor )

  def test_osflavor_unix( self ):
    nm = NativeMachine()
    file_content = 'something\nSuSE 10.2\nsomething'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data=file_content)):
      result = nm.OSFlavour() # Extracts version number from 'MyCoolOS 20.12' strings
      assertEqualsImproved( result, 'SuSE', self )
      assertEqualsImproved( nm._osflavor, 'SuSE', self )
      assertEqualsImproved( nm._osversion, '10.2', self )

  def test_osflavor_unix_with_teststr( self ):
    nm = NativeMachine()
    file_content = ''
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data = file_content)):
      result = nm.OSFlavour( 'ubunTu 13.21' )
      assertEqualsImproved( result, 'Ubuntu', self )
      assertEqualsImproved( nm._osflavor, 'Ubuntu', self )
      assertEqualsImproved( nm._osversion, '13.21', self )

  def test_osversion_win( self ):
    nm = get_win32_machine()
    nm._sysinfo =  ( 'Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel' )
    result = nm.OSVersion( 3 )
    assertEqualsImproved( result, '6.1.7600', self )
    assertEqualsImproved( nm._osversion, '6.1.7600', self )

  def test_osversion_win_position_0( self ):
    nm = get_win32_machine()
    nm._sysinfo =  ( 'Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel' )
    result = nm.OSVersion( 0 )
    assertEqualsImproved( result, '6.1.7600', self ) # `if position` ignores pos=0
    assertEqualsImproved( nm._osversion, '6.1.7600', self )

  def test_osversion_win_position_1( self ):
    nm = get_win32_machine()
    nm._sysinfo =  ( 'Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel' )
    result = nm.OSVersion( 1 )
    assertEqualsImproved( result, '6', self )
    assertEqualsImproved( nm._osversion, '6.1.7600', self )

  def test_osversion_win_position_2( self ):
    nm = get_win32_machine()
    nm._sysinfo =  ( 'Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel' )
    result = nm.OSVersion( 2 )
    assertEqualsImproved( result, '6.1', self )
    assertEqualsImproved( nm._osversion, '6.1.7600', self )

  def test_osversion_win_position_4( self ):
    nm = get_win32_machine()
    nm._sysinfo =  ( 'Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel' )
    result = nm.OSVersion( 4 )
    assertEqualsImproved( result, '6.1.7600', self )
    assertEqualsImproved( nm._osversion, '6.1.7600', self )

  def test_osversion_sun( self ):
    nm = get_win32_machine()
    nm._ostype = 'SunOS'
    result = nm.OSVersion( 2 )
    assertEqualsImproved( result, '4.x', self )
    assertEqualsImproved( nm._osversion, '4.x', self )

  def test_osversion_sun_position( self ):
    nm = get_win32_machine()
    nm._ostype = 'SunOS'
    result = nm.OSVersion( 1 )
    assertEqualsImproved( result, '4', self )
    assertEqualsImproved( nm._osversion, '4.x', self )

  def test_osversion_darwin( self ):
    mach = get_naked_machine()
    mach._ostype = 'Darwin'
    mach._sysinfo =  ( 'Darwin', 'hubert.local', '11.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386' )
    result = mach.OSVersion( 3 )
    assertEqualsImproved( result, '10.7.4', self )
    assertEqualsImproved( mach._osversion, '10.7.4', self )

  def test_osversion_linux( self ):
    mach = get_naked_machine()
    mach._ostype = 'Linux'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='somethingsomething\nUbuntu 19.3')):
      result = mach.OSVersion( 5 )
      assertEqualsImproved( result, '19.3', self )
      assertEqualsImproved( mach._osversion, '19.3', self )

  def test_osversion_linux_with_teststring( self ):
    mach = get_naked_machine()
    mach._ostype = 'Linux'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='somethingsomething\nUbuntu 19.3')):
      result = mach.OSVersion( 2, 'SuSE 3.6' )
      assertEqualsImproved( result, '3.6', self )
      assertEqualsImproved( mach._osversion, '3.6', self )

  def test_osversion_already_set( self ):
    mach = get_naked_machine()
    mach._osversion = '151.21.5'
    res_1 = mach.OSVersion()
    res_2 = mach.OSVersion( 10000 )
    res_3 = mach.OSVersion( 2 )
    assertEqualsImproved( res_1, '151.21.5', self )
    assertEqualsImproved( res_2, '151.21.5', self )
    assertEqualsImproved( res_3, '151.21', self )
    assertEqualsImproved( mach._osversion, '151.21.5', self )

  def test_osversion_no_match( self ):
    mach = get_naked_machine()
    mach._ostype = 'Linux'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='somethingsomething\nUbuntu 19.3')):
      result = mach.OSVersion( teststring = 'wrong_file123' )
      self.assertIsNone( result )
      self.assertIsNone( mach._osversion )

  def test_native_compiler_version_win( self ):
    win32_mach = get_win32_machine()
    result = win32_mach.nativeCompilerVersion()
    assertEqualsImproved( result, 'vc71', self )
    assertEqualsImproved( win32_mach._compversion, 'vc71', self )

  def test_native_compiler_version_unix( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'something', 'g++ (GCC) 4.4.7 20120313 (Red Hat 4.4.7-17)', 'Copyright (C) 2010 Free Software Foundation, Inc.', 'This is free software; see the source for copying conditions.  There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)) as popen_mock:
      result = unix_mach.nativeCompilerVersion( 2 )
      assertEqualsImproved( result, '4.4', self )
      popen_mock.assert_called_once_with( 'g++ --version' )
      assertEqualsImproved( unix_mach._compversion, '4.4.7', self )

  def test_native_compiler_version_unix_2( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'something', 'g++ (GCC) 5.2.1 20120313 (Red Hat 5.2.1-17)', 'Copyright (C) 2010 Free Software Foundation, Inc.', 'This is free software; see the source for copying conditions.  There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)) as popen_mock:
      result = unix_mach.nativeCompilerVersion( 3 )
      assertEqualsImproved( result, '5.2.1', self )
      popen_mock.assert_called_once_with( 'g++ --version' )
      assertEqualsImproved( unix_mach._compversion, '5.2.1', self )

  def test_native_compiler_version_preset( self ):
    unix_mach = get_win32_machine()
    unix_mach.nativeCompilerVersion()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'something', 'g++ (GCC) 4.4.7 20120313 (Red Hat 4.4.7-17)', 'Copyright (C) 2010 Free Software Foundation, Inc.', 'This is free software; see the source for copying conditions.  There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompilerVersion( 2 )
      assertEqualsImproved( result, 'vc71', self )
      assertEqualsImproved( unix_mach._compversion, 'vc71', self )

  def test_native_compiler_version_nomatch( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'Malformed_Version_String' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompilerVersion()
      self.assertIsNone( result )
      self.assertIsNone( unix_mach._compversion )

  def test_native_compiler_win( self ):
    win_mach = get_win32_machine()
    result = win_mach.nativeCompiler()
    assertEqualsImproved( result, 'vc71', self )
    assertEqualsImproved( win_mach._compiler, 'vc71', self )

  def test_native_compiler_preset( self ):
    win_mach = get_win32_machine()
    win_mach.nativeCompiler()
    win_mach._ostype = 'Linux'
    result = win_mach.nativeCompiler()
    assertEqualsImproved( result, 'vc71', self )
    assertEqualsImproved( win_mach._compiler, 'vc71', self )

  def test_native_compiler_unix( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'g++ (GCC) 4.4.7 20120313 (Red Hat 4.4.7-17)' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompiler()
      assertEqualsImproved( result, 'gcc44', self )
      assertEqualsImproved( unix_mach._compiler, 'gcc44', self )

  def test_native_compiler_unix_v3( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'g++ (GCC) 3.3.7 20120313 (Red Hat 3.3.7-17)' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompiler()
      assertEqualsImproved( result, 'gcc337', self )
      assertEqualsImproved( unix_mach._compiler, 'gcc337', self )

  def test_native_compiler_unix_v34( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Linux'
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'g++ (GCC) 3.4.7 20120313 (Red Hat 3.4.7-17)' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompiler()
      assertEqualsImproved( result, 'gcc34', self )
      assertEqualsImproved( unix_mach._compiler, 'gcc34', self )

  def test_native_compiler_darwin( self ):
    unix_mach = get_win32_machine()
    unix_mach._ostype = 'Darwin'
    unix_mach._sysinfo = ('Darwin', 'hubert.local', '9.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386')
    proc_mock = Mock()
    proc_mock.readlines.return_value = [ 'g++ (GCC) 6.3.1 20120313 (Red Hat 6.3.1-17)' ]
    with patch('%s.os.popen' % MODULE_NAME, new=Mock(return_value=proc_mock)):
      result = unix_mach.nativeCompiler()
      assertEqualsImproved( result, 'gcc631', self )
      assertEqualsImproved( unix_mach._compiler, 'gcc631', self )

  def test_cmt_architecture( self ):
    unix_mach = get_naked_machine()
    unix_mach._machine = 'i686'
    res_1 = unix_mach.CMTArchitecture()
    unix_mach._machine = 'x86_64'
    res_2 = unix_mach.CMTArchitecture()
    unix_mach._machine = 'ia64'
    res_3 = unix_mach.CMTArchitecture()
    unix_mach._machine = 'Power MAC'
    res_4 = unix_mach.CMTArchitecture()
    unix_mach._ostype = 'Windows'
    unix_mach._machine = 'unknown'
    with patch('%s.sys.platform' % MODULE_NAME, new='mytestarchitecture'):
      res_5 = unix_mach.CMTArchitecture()
    unix_mach._ostype = 'test'
    res_6 = unix_mach.CMTArchitecture()

    assertEqualsImproved( res_1, 'ia32', self )
    assertEqualsImproved( res_2, 'amd64', self )
    assertEqualsImproved( res_3, 'ia64', self )
    assertEqualsImproved( res_4, 'ppc', self )
    assertEqualsImproved( res_5, 'mytestarchitecture', self )
    assertEqualsImproved( res_6, 'ia32', self )

  def test_cmt_system( self ):
    unix_mach = get_naked_machine()
    unix_mach._ostype = 'Windows'
    res_1 = unix_mach.CMTSystem()
    unix_mach._ostype = 'Darwin'
    res_2 = unix_mach.CMTSystem()
    unix_mach._ostype = 'TestOS'
    unix_mach._machine = 'i386'
    res_3 = unix_mach.CMTSystem()
    unix_mach._machine = 'testArch'
    res_4 = unix_mach.CMTSystem()

    assertEqualsImproved( res_1, 'VisualC', self )
    assertEqualsImproved( res_2, 'Darwin-i386', self )
    assertEqualsImproved( res_3, 'TestOS-i386', self )
    assertEqualsImproved( res_4, 'TestOS-testArch', self )

  def test_cmt_osflavor( self ):
    unix_mach = get_naked_machine()
    unix_mach._ostype = 'Windows'
    unix_mach._arch = 'mytestarch'
    res_1 = unix_mach.CMTOSFlavour()
    assertEqualsImproved( res_1, 'winmytestarch', self )
    unix_mach._ostype = 'Darwin'
    unix_mach._sysinfo =  ( 'Darwin', 'hubert.local', '11.4.2', 'Darwin Kernel Version 11.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386' )
    res_2 = unix_mach.CMTOSFlavour()
    assertEqualsImproved( res_2, 'osx107', self )
    unix_mach._ostype = 'Linux'
    unix_mach._osflavor = 'Ubuntu'
    unix_mach._osversion = '14.04'
    res_3 = unix_mach.CMTOSFlavour()
    assertEqualsImproved( res_3, 'ub14', self )
    unix_mach._ostype = 'Linux'
    unix_mach._osflavor = 'SuSE'
    unix_mach._osversion = '11.04'
    res_4 = unix_mach.CMTOSFlavour()
    assertEqualsImproved( res_4, 'suse11', self )
    unix_mach._osversion = None
    unix_mach._ostype = 'Linux'
    unix_mach._osflavor = 'Redhat'
    unix_mach._osversion = '17.07'
    res_5 = unix_mach.CMTOSFlavour()
    assertEqualsImproved( res_5, 'rh17.07', self )

  def test_cmt_osequivalent_flavor( self ):
    unix_mach = get_naked_machine()
    unix_mach._ostype = 'Darwin'
    unix_mach._sysinfo =  ( 'Darwin', 'hubert.local', '9.4.2', 'Darwin Kernel Version 9.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386' )
    assertEqualsImproved( unix_mach.CMTOSEquivalentFlavour(), 'osx105', self )

  # def test_cmt_compatible_config( self ):
  # Uninteresting for unittests.

  def test_cmt_supported_config( self ):
    unix_mach = get_naked_machine()
    unix_mach._machine = 'x86_64'
    unix_mach._ostype = 'Darwin'
    unix_mach._sysinfo =  ( 'Darwin', 'hubert.local', '9.4.2', 'Darwin Kernel Version 9.4.2: Thu Aug 23 16:25:48 PDT 2012; root:xnu-1699.32.7~1/RELEASE_X86_64', 'x86_64', 'i386' )
    assertEqualsImproved( unix_mach.CMTSupportedConfig(), [], self )

  def test_cmt_native_config( self ):
    unix_mach = get_naked_machine()
    unix_mach._machine = 'x86_64'
    with patch('%s.NativeMachine.nativeCompiler' % MODULE_NAME, new=Mock(return_value='vc71')), patch('%s.NativeMachine.CMTOSFlavour' % MODULE_NAME, new=Mock(return_value='ub14')):
      result = unix_mach.CMTNativeConfig()
      assertEqualsImproved( result, 'x86_64-ub14-vc71-opt', self )


def get_win32_machine():
  win32_plat = Mock()
  win32_plat.uname.return_value=[ 'mysystem' ]
  # Example 'real' return value ('Windows', 'dhellmann', '2008ServerR2', '6.1.7600', 'AMD64', 'Intel64 Family 6 Model 15 Stepping 11, GenuineIntel')
  with patch('%s.platform' % MODULE_NAME, new=win32_plat), \
       patch('%s.sys.platform' % MODULE_NAME, new='win32'):
    mach_win32 = NativeMachine()
  return mach_win32

def get_win64_machine():
  with patch('%s.platform' % MODULE_NAME, new='abc'), \
       patch('%s.sys.platform' % MODULE_NAME, new='win64'):
    mach_win64 = NativeMachine()
  return mach_win64

def get_naked_machine():
  naked_plat = Mock()
  naked_plat.uname.return_value = ( 'SecretOS', None, None, None, None, None )
  with patch('%s.platform' % MODULE_NAME, new=naked_plat), \
       patch('%s.sys.platform' % MODULE_NAME, new='SecretOS'):
    mach_naked = NativeMachine()
  return mach_naked


