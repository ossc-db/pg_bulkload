How to build with Microsoft Visual C++ Express 2005

You might need:
  1. Register PostgreSQL directory to your environment.
  2. Add empty header files into your include directory.
  3. Resolve redefinitions of ERROR macro.

----
1. Register PostgreSQL directory to your environment.

The directory configuration options are found in:
  Tool > Option > Projects and Solutions > VC++ directory

You might need to add the following directories:
  into "include files"
    - C:\Program Files\PostgreSQL\8.4\include
    - C:\Program Files\PostgreSQL\8.4\include\server
    - C:\Program Files\PostgreSQL\8.4\include\internal
  into "library files"
    - C:\Program Files\PostgreSQL\8.4\lib

----
2. Add empty header files into your include directory.

PostgreSQL somehow requires non-standard posix header files.
You might need to create the following directories and header files
in your include directory. The all header files can be empty.

+- dirent.h
+- netdb.h
+- pwd.h
|
+- arpa
|    +- inet.h
|
+- netinet
|    +- in.h
|
+- sys
     +- socket.h
     +- time.h

----
3. Resolve redefinitions of ERROR macro.

It might be a bad manner, but I'll recommend to modify your wingdi.h.

--- wingdi.h       2008-01-18 22:17:42.000000000 +0900
+++ wingdi.fixed.h 2010-03-03 09:51:43.015625000 +0900
@@ -101,11 +101,10 @@
 #endif // (_WIN32_WINNT >= _WIN32_WINNT_WINXP)

 /* Region Flags */
-#define ERROR               0
+#define RGN_ERROR           0
 #define NULLREGION          1
 #define SIMPLEREGION        2
 #define COMPLEXREGION       3
-#define RGN_ERROR ERROR

 /* CombineRgn() Styles */
 #define RGN_AND             1
