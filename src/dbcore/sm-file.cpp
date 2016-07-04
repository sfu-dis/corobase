#include "sm-file.h"  
std::unordered_map<std::string, sm_file_descriptor*> sm_file_mgr::name_map;
std::unordered_map<FID, sm_file_descriptor*> sm_file_mgr::fid_map;
