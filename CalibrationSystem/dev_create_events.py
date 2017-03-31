#!/usr/bin/env python
""" Python script to generate 10x100 events for each of the required particles """

ILCSOFT_DIR = ''


def main():
  """ Main method, runs the generation process

  :returns: exit status. 0 on success, 1 on failure
  :rtype: int
  """
  # Form: ( particle_name, energy_in_gev ), both strings
  particles = [('gamma', '10'), ('mu-', '10'), ('kaon0L', '50')]
  number_of_iterations = 10  # 10 will generate 1000 = 10 x 100 events per particle
  for (particle_name, particle_energy) in particles:
    for i in xrange(0, number_of_iterations):
      run_script(particle_name, particle_energy, i)
  return 0


def run_script(particle_name, particle_energy, index):
  """ Runs the script using the passed parameters

  :param string particle_name: Name of the particle
  :param string particle_energy: Energy of the particle
  :param int index: index of the inner loop, used to index the file
  :returns: None
  :rtype: None
  """
  import subprocess
  res = subprocess.check_output(
      ['ddsim', '--steeringFile', '%s/ClicPerformance/HEAD/examples/clic_steer.py' % ILCSOFT_DIR,
       '--compactFile', '/cvmfs/clicdp.cern.ch/iLCSoft/builds/2017-02-17/x86_64-slc6-gcc62-opt/lcgeo/HEAD/CLIC/compact/CLIC_o3_v08/CLIC_o3_v08.xml', '--enableGun', '--gun.particle', particle_name, '--gun.energy',
       '%s*GeV' % particle_energy, '--gun.distribution', 'uniform',
       '--outputFile', '/afs/cern.ch/user/j/jebbing/particles/CLIC_o3_v08/%s/%s/event_%d.slcio' % (particle_name, particle_energy, index), '--numberOfEvents', '100'])
  print res


if __name__ == '__main__':
  import os
  if 'ILCSOFT' not in os.environ:
    print 'Environment variable ILCSOFT not set, please run init_ilcsoft.sh'
    exit(1)
  ILCSOFT_DIR = os.environ['ILCSOFT']

  exit(main())
