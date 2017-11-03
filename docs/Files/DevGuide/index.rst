Developer Guide
===============

Here only some things of interest for the iLCDirac development are listed. Refer
to the DIRAC :doc:`DeveloperGuide/index` for a more thourough explanation.

Developer installation
----------------------

See the DIRAC guide to use python ``virtualenv`` to develop code for DIRAC
(:doc:`DeveloperGuide/DevelopmentEnvironment/DeveloperInstallation/editingCode`)
Or install a DIRAC client and replace the source code with a git clone of
iLCDirac and DIRAC::

  wget -O dirac-install -np  https://raw.github.com/DIRACGrid/DIRAC/master/Core/scripts/dirac-install.py  --no-check-certificate
  chmod +x dirac-install
  ./dirac-install -V ILCDIRAC 
  mv DIRAC DIRAC_BAK
  mv ILCDIRAC ILCDIRAC_BAK
  mkdir DIRAC
  cd DIRAC
  git clone https://github.com/DIRACGrid/DIRAC.git .
  git remote set-url --push origin https://github.com/DIRACGrid/DIRAC.git
  git checkout origin/rel-v6r17
  cd ..
  mkdir ILCDIRAC
  cd ILCDIRAC
  git clone https://gitlab.cern.ch/CLICdp/ILCDIRAC/ILCDIRAC.git .
  git checkout -t origin/Rel-v27r0
  cd .. # DIRAC-Install home
  source bashrc
  dirac-deploy-scripts
  ## Configure for ILC-Development to use the development installation other with ILC-Production
  dirac-configure -S ILC-Development -C dips://voilcdirac01.cern.ch:9135/Configuration/Server --SkipCAChecks

Git Workflow
------------

We use the "NOSwitchYard" git workflow. Basically:

* Use a feature branch for your developments
* Never ever merge, always rebase
* Only touch lines for significant changes: ``git add -p``
* No automatic cleanup, no automatic formatting

configure git accordingly::

  git config --global branch.autosetuprebase=always
  git config --global pull.rebase=true
  

Coding Conventions
------------------

We are following the DIRAC :doc:`DeveloperGuide/CodingConvention/index`.

Use flake8 and its git-hook to ensure compliance of any touched line of code::
  
  pip install flake8
  flake8 --install-hook git
  git config --global flake8.strict true
  
This doesn't allow commit if something is wrong. Use::

  git commit -m"message" --no-verify

to overrule, but only if the messages are about lines not touched in the current
commit, which can be checked with the following git aliases, add them to your ``.gitconfig``::

  [alias]
    flakeS = "!git diff -U0 --staged | flake8 --diff"
    flake = "!git diff -U0 | flake8 --diff"

``git flakeS`` works on staged changes, ``git flake`` on unstaged changes. These
will print out any wrong formatting on staged changes. The iLCDirac continuous
integration system will ensure all lines have been formatted correctly.
