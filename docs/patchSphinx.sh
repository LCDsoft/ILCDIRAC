#!/usr/bin/env bash
# """
# find the python autodoc extension and patch it to be able to parse dirac scripts
# """

SPHINXBIN=$( which sphinx-apidoc )
SPHINXBASE=$( dirname $( dirname $SPHINXBIN ) )

for LIB in  lib lib64; do
    SPHINXEXT="$SPHINXBASE/${LIB}/python2.7/site-packages/sphinx/ext"
    AUTODOC=$SPHINXEXT/autodoc/__init__.py

    cat > autodoc.patch << EOF
--- autodoc.py~        2015-11-09 20:53:34.104991094 +0100
+++ autodoc.py 2015-11-09 20:57:02.519263402 +0100
@@ -56,9 +56,9 @@

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
done
