/*
 * ParamReader.cpp
 *
 *  Created on: Dec 14, 2017
 *      Author: vincent
 */

#include "ParamReader.h"
ParamReader::ParamReader()
{
	// TODO Auto-generated constructor stub

}

ParamReader::~ParamReader()
{
	// TODO Auto-generated destructor stub
}

ParamReader &ParamReader::open(const char* filename)
{
	ifstream in(filename);
	while (getline(in, inputStr))
	{
		if (inputStr[0] == '#')
		{
			continue;
		}
		inputContent.push_back(this->removeSpaces(inputStr));
	}
	in.close();
	return *this;
}

std::map<string, string> ParamReader::strings()
{
	std::map<string, string> param_pair;
	string tmp, key_tmp, value_tmp;
	long unsigned int equal_symbol_index = 0;
	for (long unsigned  int i = 0; i < this->inputContent.size(); i++)
	{
		tmp = this->inputContent[i];
		equal_symbol_index = tmp.find("=");
		key_tmp = tmp.substr(0, equal_symbol_index);
		value_tmp = tmp.substr(equal_symbol_index + 1, tmp.length() - 1);
		if (this->is_number(value_tmp))
		{
			continue;
		}
		param_pair[key_tmp] = value_tmp;
	}
	return param_pair;
}

std::map<string, float> ParamReader::numbers()
{
	std::map<string, float> param_pair;
	string tmp, key_tmp, value_tmp;
	long unsigned int equal_symbol_index = 0;
	for (long unsigned int i = 0; i < this->inputContent.size(); i++)
	{
		tmp = this->inputContent[i];
		equal_symbol_index = tmp.find("=");
		key_tmp = tmp.substr(0, equal_symbol_index);
		value_tmp = tmp.substr(equal_symbol_index + 1, tmp.length() - 1);
		param_pair[key_tmp] = (float)atof(value_tmp.c_str());
	}
	return param_pair;
}

string ParamReader::removeSpaces(string input)
{
	input.erase(std::remove(input.begin(), input.end(), ' '), input.end());
	return input;
}

bool ParamReader::is_number(const std::string& s)
{
	std::string::const_iterator it = s.begin();
	while (it != s.end() && std::isdigit(*it))
		++it;
	return !s.empty() && it == s.end();

}
