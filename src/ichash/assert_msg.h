#ifndef RESEARCH_ASSERT_MSG_H
#define RESEARCH_ASSERT_MSG_H
#include "assert.h"
#include <iostream>

#define ASSERT(exp_,msg_) if(!(exp_)) {std::cerr<<"ASSERT FALSE : "<<msg_<<std::endl;assert(false);}

#endif //RESEARCH_ASSERT_MSG_H
