#!/usr/bin/env bash
# """
# find the python autodoc extension and patch it to be able to parse dirac scripts
# """

SPHINXBIN=$( which sphinx-apidoc )
SPHINXBASE=$( dirname $( dirname $SPHINXBIN ) )
SPHINXEXT="$SPHINXBASE/lib/python2.7/site-packages/sphinx/ext"
AUTODOC=$SPHINXEXT/autodoc.py

cat > autodoc.patch << EOF
--- /home/sailer/software/DIRAC/DiracDevV6r13/Linux_x86_64_glibc-2.12/lib/python2.7/site-packages/sphinx/ext/autodoc.py~        2015-11-09 13:53:34.104991094 +0100
+++ /home/sailer/software/DIRAC/DiracDevV6r13/Linux_x86_64_glibc-2.12/lib/python2.7/site-packages/sphinx/ext/autodoc.py 2015-11-09 13:57:02.519263402 +0100
@@ -36,9 +36,9 @@

 #: extended signature RE: with explicit module name separated by ::
 py_ext_sig_re = re.compile(
-    r'''^ ([\w.]+::)?            # explicit module name
-          ([\w.]+\.)?            # module and/or class name(s)
-          (\w+)  \s*             # thing name
+    r'''^ ([\w.-]+::)?           # explicit module name
+          ([\w.-]+\.)?           # module and/or class name(s)
+          ([\w-]+)  \s*          # thing name
           (?: \((.*)\)           # optional: arguments
            (?:\s* -> \s* (.*))?  #           return annotation
           )? $                   # and nothing more
EOF

patch -N $AUTODOC < autodoc.patch

rm autodoc.patch

echo Patched this file: $AUTODOC
