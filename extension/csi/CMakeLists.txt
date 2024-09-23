cmake_minimum_required(VERSION 3.5...3.29)

project(CSIExtension)

include_directories(include)

set(CSI_EXTENSION_FILES
    csi_extension.cpp
)

build_static_extension(csi ${CSI_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(csi ${PARAMETERS} ${CSI_EXTENSION_FILES})

install(
  TARGETS csi_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
