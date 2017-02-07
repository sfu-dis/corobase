#include "sm-index.h"  
std::unordered_map<std::string, sm_index_descriptor*> sm_index_mgr::name_map;
std::unordered_map<FID, sm_index_descriptor*> sm_index_mgr::fid_map;
