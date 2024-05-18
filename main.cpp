#include<iostream>
#include<fstream>
#include<string>
#include<vector>
#include<unordered_map>
#include<sstream>
#include<iomanip>
#include<queue>

std::fstream schema;
std::fstream mytables;

class INTERPRETER
{
    private:
        std::string query;
        std::string token;
        std::vector<std::string> token_Buffer;
        std::vector<std::string> tables;
        std::string command;
        std::string table_name;
        std::vector<std::string> col_names;
        std::string record;
        std::vector<std::string>record_vector;
        int primary_key_column = -1;
        int foreign_key_column;
        std::string fk_column_name;
        std::string reference_table_name;
        std::string fk_reference_column_name;
        std::string pk_column_name = "NULL";

    
    public:

        INTERPRETER()
        {
            this->query = "";
            this->command = "";
            this->table_name = "";
            this->record = "";
        }

        //std::fstream schema_fin;
        //std::fstream schema_fout;

        void LOAD_QUERY(std::string);
        void EXECUTE_QUERY();
        void TOKENIZE();
        void CREATE();
        void CREATE_WITH_PRIMARY_KEY();
        void SELECT();
        void DESCRIBE();
        void HELP();
        void INSERT();
        void DELETE();
        void DROP();
        void TERMINATE();
        void TOKENIZE_SCHEMA();
        void UPDATE();
        void ALTER();
        void SELECT_WITH_WHERE();
        void HELP_TABLES();

};

void INTERPRETER::LOAD_QUERY(std::string query)
{
    this->query = query;
    token_Buffer.clear();
    
    //check_semicolon
    if(query[query.length() - 1] != ';')
    {
        std::cout<<"ERROR: Expected ; at the end of query!"<<std::endl;
        return;
    }

    TOKENIZE();
    EXECUTE_QUERY();
}

void INTERPRETER::TOKENIZE()
{
    std::stringstream check1(query);
    while(getline(check1, token, ' '))
    {
        token_Buffer.push_back(token);
    }
    command = token_Buffer[0];

    //To print token Buffer
    // for(int i = 0; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

}

void INTERPRETER::EXECUTE_QUERY()
{

    if(command == "CREATE")
    {
        if(token_Buffer[1] == "TABLE")
        {
            if(token_Buffer[token_Buffer.size() - 3] == "PRIMARY" && token_Buffer[token_Buffer.size() - 2] == "KEY")
                CREATE_WITH_PRIMARY_KEY();          
            // else if(token_Buffer[token_Buffer.size() - 9] == "PRIMARY" && token_Buffer[token_Buffer.size() - 8] == "KEY")
            // {
            //     if(token_Buffer[token_Buffer.size() - 6] == "FOREIGN" && token_Buffer[token_Buffer.size() - 5] == "KEY")
            //         CREATE_WITH_BOTH();
            // }
            // else if(token_Buffer[token_Buffer.size() - 6] == "FOREIGN" && token_Buffer[token_Buffer.size() - 5] == "KEY")
            //     CREATE_WITH_FOREIGN_KEY();
            else
                CREATE();
        }
        else
        {
            std::cout<<"ERROR: EXPECTED \'TABLE\' INSTEAD OF \'"<<token_Buffer[1]<<"\'!"<<std::endl<<std::endl;
            return;
        }
    }
    
    else if(command == "DROP")
    {
        if(token_Buffer[1] == "TABLE")
            DROP();
        else
        {
            std::cout<<"ERROR: EXPECTED \'TABLE\' INSTEAD OF \'"<<token_Buffer[1]<<"\'!"<<std::endl<<std::endl;
            return;
        }
    }

    else if(command == "SELECT")
    {
        if(token_Buffer[token_Buffer.size() - 2] == "FROM")
            SELECT();

        else if(token_Buffer[token_Buffer.size() - 4] == "WHERE")
            SELECT_WITH_WHERE(); 
        else
        {
            std::cout<<"ERROR: EXPECTED \'FROM\' INSTEAD OF \'"<<token_Buffer[token_Buffer.size() - 2]<<"\'!"<<std::endl<<std::endl;
            return;
        }
    }


    else if(command == "DELETE")
    {
        if(token_Buffer[1] == "FROM")
            DELETE();
        else
        {
            std::cout<<"ERROR: EXPECTED \'FROM\' INSTEAD OF \'"<<token_Buffer[1]<<"\'!"<<std::endl<<std::endl;
            return;
        }
    }

    else if(command == "INSERT")
    {
        if(token_Buffer[1] != "INTO")
            std::cout<<"ERROR: EXPECTED \'INTO\' INSTEAD OF \'"<<token_Buffer[1]<<"\'!"<<std::endl<<std::endl;
        
        if(token_Buffer[3] != "VALUES(")
            std::cout<<"ERROR: EXPECTED \'VALUES(\' INSTEAD OF \'"<<token_Buffer[3]<<"\'!"<<std::endl<<std::endl;
        
        if(token_Buffer[1] == "INTO" && token_Buffer[3] == "VALUES(")
            INSERT();
    }
    
    else if(command == "HELP;" || command == "HELP")
    {
        HELP();
    }

    else if(command == "HELP_TABLES")
    {
        HELP_TABLES();
    }
    
    else if(command == "DESCRIBE")
    {
        DESCRIBE();
    }

    else if(command == "UPDATE")
    {
        if(token_Buffer[2] != "SET")
            std::cout<<"ERROR: EXPECTED \'SET\' INSTEAD OF \'"<<token_Buffer[2]<<"\'!"<<std::endl<<std::endl;
        
        if(token_Buffer[6] != "WHERE")
            std::cout<<"ERROR: EXPECTED \'WHERE\' INSTEAD OF \'"<<token_Buffer[6]<<"\'!"<<std::endl<<std::endl;
        
        if(token_Buffer[2] == "SET" && token_Buffer[6] == "WHERE")
            UPDATE();
    }

    else if(command == "ALTER")
    {
        if(token_Buffer[1] == "TABLE")
            ALTER();

        else
            std::cout<<"ERROR!"<<std::endl<<std::endl;
    }

    else if(command == "QUIT")
    {
        std::cout<<" EXITING SQL";
        exit(0);
    }

    else 
    {
        std::cout<<"ERROR: INVALID SQL COMMAND!"<<std::endl<<std::endl;
    }
}

void INTERPRETER::TOKENIZE_SCHEMA()
{  
    token_Buffer.clear();
    while(!schema.eof())
    {
        getline(schema, record);

        if(record.substr(0, table_name.length()) == table_name)
        {
            std::stringstream check1(record);

            while(getline(check1, token, '#'))
            {
                token_Buffer.push_back(token);
            }
        }
    }

    // for(int i = 0; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

}

void INTERPRETER::ALTER()
{
    table_name = token_Buffer[2];

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
        return;
    }

    std::string new_column = token_Buffer[token_Buffer.size() - 2];
    std::string new_datatype = token_Buffer[token_Buffer.size() - 1];
    
    std::string::iterator itr = new_datatype.end();
    new_datatype.erase(itr - 1, itr);

    schema.close();
    schema.open("SCHEMA.txt", std::ios::in);

    std::vector<std::string> row_vector;

    while(!schema.eof())
    {
        getline(schema, record);

        if(record.substr(0, table_name.length()) == table_name)
            row_vector.push_back(record);
    }

    std::string new_record = table_name + "#" + new_column + "#" + new_datatype;

    row_vector.push_back(new_record);

    schema.close();

    schema.open("SCHEMA.txt", std::ios::in);

    std::fstream schema_new("SCHEMA_new.txt", std::ios::app);

    while(!schema.eof())
    {
        getline(schema, record);

        if(record.substr(0, table_name.length()) == table_name || record == "")
            continue;

        schema_new << record << std::endl;
    }

    for(int i = 0; i < row_vector.size(); i++)
    {
        schema_new << row_vector[i] << std::endl;
    }
    std::cout<< "\nTable Altered!\n";
    schema_new.close();
    schema.close();

    remove("SCHEMA.txt");
    rename("SCHEMA_new.txt", "SCHEMA.txt");

    
    std::fstream dataFile_new;

    dataFile.close();
    dataFile.open(table_name, std::ios::in);
    dataFile_new.open(table_name + "_new", std::ios::out);

    getline(dataFile, record);
    record += new_column + "#";
    dataFile_new << record<<std::endl;

    while(!dataFile.eof())
    {
        getline(dataFile, record);
        if(record == "")
            continue;
        record += "NULL#";
        dataFile_new << record <<std::endl;
    }

    dataFile_new.close();
    dataFile.close();

    remove(table_name.c_str());
    rename((table_name + "_new").c_str(), table_name.c_str());
}

void INTERPRETER::SELECT()
{
    table_name = token_Buffer[token_Buffer.size() - 1];
    table_name.erase(table_name.size() - 1, table_name.size());

    // for(int i = 0; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

    std::fstream dataFile(table_name, std::ios::in);
    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
        return;
    }

    if(token_Buffer[1] == "*")
    {
        std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

        getline(dataFile, record);
        record_vector.clear();

        std::stringstream check5(record);

        while(getline(check5, token, '#'))
            record_vector.push_back(token);

        for(int i = 0; i < record_vector.size(); i++)
        {
            std::cout<<std::setw(20)<<record_vector[i]<<" | ";
        }
        std::cout<<std::endl<<std::endl;

        while(!dataFile.eof())
        {
            getline(dataFile, record);

            record_vector.clear();

            std::stringstream check1(record);

            while(getline(check1, token, '#'))
            {
                record_vector.push_back(token);
            }

            for(int i = 0; i < record_vector.size(); i++)
            {
                std::cout<<std::setw(20)<<record_vector[i]<<" | ";
            }
            std::cout<<std::endl;
        }
        std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;
        std::cout<<std::endl;

    }
    else
    {
        col_names.clear();
        std::vector<int> col_indices;

        for(int i = 1; i < token_Buffer.size() - 2; i++)
            col_names.push_back(token_Buffer[i]);

        for(int i = 0; i < col_names.size() - 1; i++)
            col_names[i].erase(col_names[i].size() - 1);
        
        getline(dataFile, record);

        std::vector<std::string> columns;

        
        std::stringstream check1(record);

        while(getline(check1, token, '#'))
        {
            columns.push_back(token);
        }

        // for(int i = 0; i < col_names.size(); i++)
        // {
        //     for(int j = 0; j < columns.size(); j++)
        //     {
        //         if()
        //     }
        // }
        
        bool check = false;

        for(int i = 0; i < col_names.size(); i++)
        {
            check = false;
            for(int j = 0; j < columns.size(); j++)
            {
                if(col_names[i] == columns[j])
                {
                    col_indices.push_back(j);
                    check = true;
                }
            }
            if(!check)
            {
                std::cout<<"ERROR: NO SUCH COLUMN IN TABLE!"<<std::endl<<std::endl;
                return;
            }
        }

        std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

        for(int i = 0; i < col_names.size(); i++)
        {
            std::cout<<std::setw(20)<<col_names[i]<<" | ";
        }
        std::cout<<std::endl<<std::endl;

        while(!dataFile.eof())
        {
            getline(dataFile, record);

            std::stringstream check2(record);

            record_vector.clear();

            while(getline(check2, token, '#'))
                record_vector.push_back(token);

            if(record == "")
                break;

            for(int i = 0; i < col_indices.size(); i++)
            {
                std::cout<<std::setw(20)<<record_vector[col_indices[i]]<<" | ";
            }
            std::cout<<std::endl; 
        }

        std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;

    }
    dataFile.close();
}

void INTERPRETER::SELECT_WITH_WHERE()
{
    table_name = token_Buffer[token_Buffer.size() - 5];
    //std::cout<<"Table Name = "<<table_name<<std::endl;
    
    std::string condition_column = token_Buffer[token_Buffer.size() - 3];

    std::string condition_value = token_Buffer[token_Buffer.size() - 1];
    condition_value.erase(condition_value.length() - 1, condition_value.length());

    int condition_column_index;

    std::string op = token_Buffer[token_Buffer.size() - 2];

    std::fstream dataFile(table_name, std::ios::in);
    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!" <<std::endl<<std::endl;
        return;
    }

    getline(dataFile, record);

    std::stringstream check1(record);

    record_vector.clear();

    while(getline(check1, token, '#'))
    {
        record_vector.push_back(token);
    }

    bool checker = false;
    for(int i = 0; i < record_vector.size(); i++)
    {
        if(record_vector[i] == condition_column)
        {
            condition_column_index = i;
            checker = true;
        }
    }

    if(!checker)
    {
        std::cout<<"ERROR: CONDITION COLUMN DOES NOT EXIST FOR TABLE!"<<std::endl<<std::endl;
        return;
    }

    if(op == "=")
    {
        if(token_Buffer[1] == "*")
        {
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

            dataFile.close();
            dataFile.open(table_name, std::ios::in);

            bool flag = true;

            for(int i = 0; i < record_vector.size(); i++)
                std::cout<<std::setw(20)<<record_vector[i]<<" | ";
            std::cout<<std::endl<<std::endl;

            while(!dataFile.eof())
            {
                getline(dataFile, record);

                std::stringstream check2(record);

                record_vector.clear();

                while(getline(check2, token, '#'))
                    record_vector.push_back(token);

                if(record == "")
                    break;

                for(int i = 0; i < record_vector.size(); i++)
                {
                    if(record_vector[condition_column_index] == condition_value)
                    {
                        std::cout<<std::setw(20)<<record_vector[i]<<" | ";
                        flag = true;
                    }
                    else
                        flag = false;
                }
                
                if(flag)                
                    std::cout<<std::endl;

            }
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;
            std::cout<<std::endl;
        }
        else
        {
            col_names.clear();
            std::vector<int> col_indices;

            for(int i = 1; i < token_Buffer.size() - 6; i++)
                col_names.push_back(token_Buffer[i]);
            
            for(int i = 0; i < col_names.size() - 1; i++)
                col_names[i].erase(col_names[i].length() -1);


            dataFile.close();
            dataFile.open(table_name, std::ios::in);

            getline(dataFile, record);

            std::vector<std::string> columns;

            std::stringstream check3(record);

            while(getline(check3, token, '#'))
            {
                columns.push_back(token);
            }

            // for(int i = 0; i < col_names.size(); i++)
            // {
            //     for(int j = 0; j < columns.size(); j++)
            //     {
            //         if(col_names[i] == columns[j])
            //             col_indices.push_back(j);
            //     }
            // }

            bool check = false;

            for(int i = 0; i < col_names.size(); i++)
            {
                check = false;
                for(int j = 0; j < columns.size(); j++)
                {
                    if(col_names[i] == columns[j])
                    {
                        col_indices.push_back(j);
                        check = true;
                    }
                }
                if(!check)
                {
                    std::cout<<"ERROR: INVALID COLUMN NAME IN TABLE!"<<std::endl<<std::endl;
                    return;
                }
            }

            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

            dataFile.close();
            dataFile.open(table_name, std::ios::in);

            bool flag;

            for(int i = 0; i < col_names.size(); i++)
                std::cout<<std::setw(20)<<col_names[i]<<" | ";
            std::cout<<std::endl<<std::endl;

            while(!dataFile.eof())
            {
                getline(dataFile, record);

                std::stringstream check4(record);
                record_vector.clear();

                while(getline(check4, token, '#'))
                    record_vector.push_back(token);
                
                if(record == "")
                    break;
                
                for(int i = 0; i < col_indices.size(); i++)
                {
                    if(record_vector[condition_column_index] == condition_value)
                    {
                        std::cout<<std::setw(20)<<record_vector[col_indices[i]]<<" | ";
                        flag = true;
                    }
                    else
                        flag = false;
                }
                if(flag)
                    std::cout<<std::endl; 

            }
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;
        }
    }

    if(op == "!=")
    {
        if(token_Buffer[1] == "*")
        {
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

            // dataFile.close();
            // dataFile.open(table_name, std::ios::in);

            bool flag = true;

            for(int i = 0; i < record_vector.size(); i++)
                std::cout<<std::setw(20)<<record_vector[i]<<" | ";
            std::cout<<std::endl<<std::endl;

            while(!dataFile.eof())
            {
                getline(dataFile, record);

                std::stringstream check2(record);

                record_vector.clear();

                while(getline(check2, token, '#'))
                    record_vector.push_back(token);

                if(record == "")
                    break;

                for(int i = 0; i < record_vector.size(); i++)
                {
                    if(record_vector[condition_column_index] != condition_value)
                    {
                        std::cout<<std::setw(20)<<record_vector[i]<<" | ";
                        flag = true;
                    }
                    else
                        flag = false;
                }
                
                if(flag)                
                    std::cout<<std::endl;

            }
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;
            std::cout<<std::endl;
        }
        else
        {
            col_names.clear();
            std::vector<int> col_indices;

            for(int i = 1; i < token_Buffer.size() - 6; i++)
                col_names.push_back(token_Buffer[i]);
            
            for(int i = 0; i < col_names.size() - 1; i++)
                col_names[i].erase(col_names[i].length() -1);


            dataFile.close();
            dataFile.open(table_name, std::ios::in);

            getline(dataFile, record);

            std::vector<std::string> columns;

            std::stringstream check3(record);

            while(getline(check3, token, '#'))
            {
                columns.push_back(token);
            }

            // for(int i = 0; i < col_names.size(); i++)
            // {
            //     for(int j = 0; j < columns.size(); j++)
            //     {
            //         if(col_names[i] == columns[j])
            //             col_indices.push_back(j);
            //     }
            // }

            bool check = false;

            for(int i = 0; i < col_names.size(); i++)
            {
                check = false;
                for(int j = 0; j < columns.size(); j++)
                {
                    if(col_names[i] == columns[j])
                    {
                        col_indices.push_back(j);
                        check = true;
                    }
                }
                if(!check)
                {
                    std::cout<<"ERROR: INVALID COLUMN NAME IN TABLE!"<<std::endl<<std::endl;
                    return;
                }
            }

            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ "<<std::endl;

            // dataFile.close();
            // dataFile.open(table_name, std::ios::in);

            bool flag;

            for(int i = 0; i < col_names.size(); i++)
                std::cout<<std::setw(20)<<col_names[i]<<" | ";
            std::cout<<std::endl<<std::endl;

            while(!dataFile.eof())
            {
                getline(dataFile, record);

                std::stringstream check4(record);
                record_vector.clear();

                while(getline(check4, token, '#'))
                    record_vector.push_back(token);
                
                if(record == "")
                    break;
                
                for(int i = 0; i < col_indices.size(); i++)
                {
                    if(record_vector[condition_column_index] != condition_value)
                    {
                        std::cout<<std::setw(20)<<record_vector[col_indices[i]]<<" | ";
                        flag = true;
                    }
                    else
                        flag = false;
                }
                if(flag)
                    std::cout<<std::endl; 

            }
            std::cout<<"_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  "<<std::endl;
        }
    }

    dataFile.close();

}

void INTERPRETER::CREATE()
{
    table_name = token_Buffer[2];
    table_name.erase(table_name.length() - 1, table_name.length());
    //std::cout<<"TABLE NAME: "<<table_name<<std::endl;

    // for(int i = 3; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

    std::unordered_map<std::string, std::string>col_var_map;

    //Remove last 2 (')' & ';') chars of last column
    std::string :: iterator itr = token_Buffer[token_Buffer.size() - 1].end();
    token_Buffer[token_Buffer.size() - 1].erase(itr - 2, itr);

    for(int i = 3; i < token_Buffer.size(); i++)
    {
        //remove comma after column type
        if(token_Buffer[i][token_Buffer[i].length() - 1] == ',')   
            token_Buffer[i].erase(token_Buffer[i].length() - 1, token_Buffer[i].length());
        
        //std::cout<<token_Buffer[i]<<std::endl;
    }

    int queried_columns_count = (token_Buffer.size() - 3) / 2;

    col_names.resize(queried_columns_count);

    for(int i = 0; i < queried_columns_count; i++)
    {
        col_names[i] = token_Buffer[2 * i + 3];
        col_var_map[col_names[i]] = token_Buffer[2 * i + 4];
    }

    std::unordered_map<std::string, std::string> :: iterator map_itr;
    std::vector<std::string> :: iterator v_itr;


    //printing the map
    // for(map_itr = col_var_map.begin(); map_itr != col_var_map.end(); map_itr++)
    // {
    //     std::cout<<map_itr->first<<"\t"<<map_itr->second<<std::endl;;
    // }

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        std::cout<<"TABLE ALREADY EXISTS!"<<std::endl;
    }
    else
    {
        dataFile.close();

        schema.open("SCHEMA.txt", std::ios::app);
        mytables.open("tables.txt",std::ios::app);
        dataFile.open(table_name, std::ios::out);

        for(int i = 0; i < col_names.size(); i++)
        {
            map_itr = col_var_map.find(col_names[i]);
            if(map_itr != col_var_map.end())
                schema<<table_name<<"#"<<map_itr->first<<"#"<<map_itr->second<<std::endl;
        }
        mytables<<std::endl << table_name;
        mytables.close();
        for(int i = 0; i < col_names.size(); i++)
        {
            dataFile<<col_names[i]<<"#";            
        }
        dataFile<<std::endl;

        schema.close();
        dataFile.close();
    }

    tables.push_back(table_name);
    std::cout<<"TABLE CREATED"<<std::endl<<std::endl;
}

void INTERPRETER::CREATE_WITH_PRIMARY_KEY()
{
    table_name = token_Buffer[2];
    table_name.erase(table_name.length() - 1, table_name.length());

    std::string :: iterator itr = token_Buffer[token_Buffer.size() - 1].end();
    token_Buffer[token_Buffer.size() - 1].erase(itr - 3, itr);
    itr = token_Buffer[token_Buffer.size() - 1].begin();
    token_Buffer[token_Buffer.size() - 1].erase(itr, itr + 1);

    for(int i = 3; i < token_Buffer.size(); i++)
    {
        //remove comma after column type
        if(token_Buffer[i][token_Buffer[i].length() - 1] == ',')   
            token_Buffer[i].erase(token_Buffer[i].length() - 1, token_Buffer[i].length());
        
        //std::cout<<token_Buffer[i]<<std::endl;
    }

    int queried_columns_count = ((token_Buffer.size() - 6) / 2);

    std::unordered_map<std::string, std::string>col_var_map;

    col_names.resize(queried_columns_count);

    for(int i = 0; i < queried_columns_count; i++)
    {
        col_names[i] = token_Buffer[2 * i + 3];
        col_var_map[col_names[i]] = token_Buffer[2 * i + 4];
    }
    
    std::unordered_map<std::string, std::string> :: iterator map_itr;
    std::vector<std::string> :: iterator v_itr;


    pk_column_name = token_Buffer[token_Buffer.size() - 1];

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        std::cout<<"TABLE ALREADY EXISTS!"<<std::endl;
        return;
    }
    else
    {
        dataFile.close();

        schema.open("SCHEMA.txt", std::ios::app);
        mytables.open("tables.txt",std::ios::app);
        dataFile.open(table_name, std::ios::out);

        for(int i = 0; i < col_names.size(); i++)
        {
            map_itr = col_var_map.find(col_names[i]);
            if(map_itr != col_var_map.end())
                schema<<table_name<<"#"<<map_itr->first<<"#"<<map_itr->second<<std::endl;
        }
        schema<<table_name<<"#"<<"PRIMARY KEY"<<"#"<<pk_column_name<<std::endl;
        mytables<<std::endl<<table_name;
        mytables.close();
        for(int i = 0; i < col_names.size(); i++)
        {
            dataFile<<col_names[i]<<"#";            
        }
        dataFile<<std::endl;
        schema.close();
        dataFile.close();
    }  
    tables.push_back(table_name);
    std::cout<<"TABLE CREATED!"<<std::endl;  

}

void INTERPRETER::DROP()
{
    table_name = token_Buffer[2];
    table_name.erase(table_name.length() - 1, table_name.length());

    //std::cout<<"TABLE NAME: "<<table_name<<std::endl;

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        dataFile.close();
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
        return;
    }


    schema.close();
    schema.open("SCHEMA.txt", std::ios::in);
    mytables.close();
    mytables.open("tables.txt", std::ios::in);

    std::fstream schema_new;
    std::fstream mytables_new;
    schema_new.open("SCHEMA_new.txt", std::ios::app);
    mytables_new.open("tablesNew.txt", std::ios::app);

    std::string mytable;

    while(!schema.eof())
    {
        getline(schema, record);

        if(record.substr(0, table_name.length()) == table_name || record == "")
        {
            continue;
        }

        schema_new << record <<std::endl;
    }

    while(!mytables.eof())
    {
        getline(mytables, mytable);
        if(mytable == table_name){
           continue;
        }else{
            mytables_new <<std::endl<< mytable ;
        }
    }

    mytables.close();
    mytables_new.close();
    schema_new.close();
    schema.close();

    remove("SCHEMA.txt");
    rename("SCHEMA_new.txt", "SCHEMA.txt");

    remove("tables.txt");
    rename("tablesNew.txt", "tables.txt");

    const char* file = table_name.c_str();
    int status = remove(file);


   //tables.erase(std::remove(tables.begin(), tables.end(), table_name.c_str()), tables.end());

    if(!status)
        std::cout<<"TABLE DROPPED!"<<std::endl<<std::endl;
    else
        std::cout<<"ERROR: TABLE WAS NOT DROPPPED!"<<std::endl<<std::endl;
}

void INTERPRETER::DESCRIBE()
{
    schema.close();
    schema.open("SCHEMA.txt", std::ios::in);
    
    table_name = token_Buffer[1];
    table_name.erase(table_name.length() - 1, table_name.length());

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        TOKENIZE_SCHEMA();

        for(int i = 0; i < token_Buffer.size() - 2; i = i + 3)
        {
            std::cout<<token_Buffer[i + 1]<<"->"<<token_Buffer[i + 2]<<std::endl;
        }
        std::cout<<std::endl;
        
        schema.close();
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
    }

    
}

void INTERPRETER::INSERT()
{
    pk_column_name = "NULL";
    
    table_name = token_Buffer[2];

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);  
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
        return;
    }

    schema.close();
    schema.open("SCHEMA.txt", std::ios::in);

    while(!schema.eof())
    {
        getline(schema, record);

        std::stringstream check1(record);
        
        record_vector.clear();

        while(getline(check1, token, '#'))
        {
            record_vector.push_back(token);
        }

        if(record_vector[0] == table_name)
        {
            if(record_vector[1] == "PRIMARY KEY")
                pk_column_name = record_vector[2];
        }
    }

    schema.close();

    if(pk_column_name != "NULL")
    {
        record_vector.clear();

        getline(dataFile, record);

        std::stringstream check2(record);

        while(getline(check2, token, '#'))
        {
            record_vector.push_back(token);
        }

        for(int i = 0; i < record_vector.size(); i++)
        {
            if(record_vector[i] == pk_column_name)
                primary_key_column = i;
        }
    }


    //std::cout<<table_name<<std::endl;

    // for(int i = 0; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

    record_vector.clear();

    for(int i = 4; i < token_Buffer.size(); i++)
        record_vector.push_back(token_Buffer[i]);
    
    for(int i = 0; i < record_vector.size() - 1; i++)
        record_vector[i].erase(record_vector[i].length() - 1, record_vector[i].length());
    
    std::string :: iterator itr = record_vector[record_vector.size() - 1].end();
    record_vector[record_vector.size() - 1].erase(itr - 2, itr);

    // for(int i = 0; i < record_vector.size(); i++)
    //     std::cout<<record_vector[i]<<std::endl;

    std::vector<std::string> new_rec_vec;

    if(pk_column_name != "NULL")
    {
        while(!dataFile.eof())
    {
        getline(dataFile, record);

        if(record == "")
            break;

        std::stringstream check3(record);

        new_rec_vec.clear();


        while(getline(check3, token, '#'))
        {
            new_rec_vec.push_back(token);
        }

        if(record_vector[primary_key_column] == new_rec_vec[primary_key_column])
        {
            std::cout<<"ERROR: PRIMARY KEY MUST BE UNIQUE!"<<std::endl;
            return;
        }
    }

        dataFile.close();
    }

    new_rec_vec.clear();
    dataFile.close();
    dataFile.open(table_name, std::ios::app);

    for(int i = 0; i < record_vector.size(); i++)
    {
        dataFile<<record_vector[i]<<"#";
    }
    dataFile<<std::endl;
    std::cout<<std::endl;

    dataFile.close();

    std::cout<<"RECORD INSERTED!"<<std::endl<<std::endl;

}

void INTERPRETER::HELP()
{
    if(token_Buffer.size() == 1)
    {
        std::cout<<"HELP"<<std::endl;
        std::cout<<"Use following help commands to get help for a particular command: "<<std::endl;
        std::cout<<"\t"<<"-- HELP CREATE --> Help for CREATE command"<<std::endl;
        std::cout<<"\t"<<"-- HELP SELECT --> Help for SELECT command"<<std::endl;
        std::cout<<"\t"<<"-- HELP DESCRIBE --> Help for DESCRIBE command"<<std::endl;
        std::cout<<"\t"<<"-- HELP DROP --> Help for DROP command"<<std::endl;
        std::cout<<"\t"<<"-- HELP DELETE --> Help for DELETE command"<<std::endl;
        std::cout<<"\t"<<"-- HELP INSERT --> Help for INSERT command"<<std::endl;
        std::cout<<"\t"<<"-- HELP ALTER --> Help for ALTER command"<<std::endl;
        std::cout<<std::endl;
        return;
    }

    else if(token_Buffer[1] == "CREATE;")
    {
        std::cout<<"CREATE is used to create a new table \n";
        std::cout<<"\t"<<"CREATE TABLE <table_name> (attr_name1 attr_type1, attr_name2 attr_type2, …);\n\n";
        return;
    }

    else if(token_Buffer[1] == "DROP;")
    {
        std::cout<<"DROP is used to delete a table along with data\n";
        std::cout<<"\t"<<"DROP TABLE <table_name>;\n\n";

        return;
    }

    else if(token_Buffer[1] == "DESCRIBE;")
    {
        std::cout<<"DESCRIBE Describes the schema of table T_NAME\n";
        std::cout<<"\t"<<"DESCRIBE <table_name>;\n\n";
        return;
    }

    else if(token_Buffer[1] == "INSERT;")
    {
        std::cout<<"INSERT is used to insert a record in the table\n";
        std::cout<<"\t"<<"INSERT INTO <table_name> VALUES(attr_value1,attr_value2, …);\n\tNote that NULL is not permitted for any attribute.\n";
        return;
    }


    else if(token_Buffer[1] == "DELETE;")
    {
        std::cout<<"DELETE is used to delete an entry from a table\n";
        std::cout<<"\t"<<"DELETE FROM <table_name> WHERE <condition_list>;\n\n";
        return;
    }



    else if(token_Buffer[1] == "SELECT;")
    {
        std::cout<<"SELECT is used to select a set of records from the given table\n";
        std::cout<<"\t"<<"SELECT * FROM <table_name>;\n";
        std::cout<<"\t\t\tOR\n";
        std::cout<<"\t"<<"SELECT * FROM <table_name> WHERE <condition_list>;\n";
        //std::cout<<"Above two commands are to select all columns from the table\n";
        std::cout<<"\t\t\tOR\n";
        std::cout<<"\t"<<"SELECT <attr_list> FROM <table_name>;\n";
        std::cout<<"\t\t\tOR\n";
        std::cout<<"\t"<<"SELECT <attr_list> FROM <table_name> WHERE <condition_list>;\n\n";
        return;
    }

    else if(token_Buffer[1] == "ALTER;")
    {
        std::cout<<"The ALTER TABLE statement is used to add columns in an existing table\n";
        std::cout<<"\t"<<"ALTER TABLE <table_name> ADD COLUMN <column_name> <column_data_type>;\n\n";
        return;
    }
    
    else
    {
        std::cout<<"NO SUCH COMMAND FOR HELP!"<<std::endl;
        std::cout<<"AVAILABLE OPTIONS: "<<std::endl;

        std::cout<<"\tHELP"<<std::endl;
        std::cout<<"\tUse following help commands to get help for a particular command: "<<std::endl;
        std::cout<<"\t\t"<<"-- HELP CREATE --> Help for CREATE command"<<std::endl;
        std::cout<<"\t\t"<<"-- HELP SELECT --> Help for SELECT command"<<std::endl;
        std::cout<<"\t\t"<<"-- HELP DESCRIBE --> Help for DESCRIBE command"<<std::endl;
        std::cout<<"\t\t"<<"-- HELP DROP --> Help for DROP command"<<std::endl;
        std::cout<<"\t\t"<<"-- HELP DELETE --> Help for DELETE command"<<std::endl;
        std::cout<<"\t\t"<<"-- HELP INSERT --> Help for INSERT command"<<std::endl;    
        std::cout<<std::endl;
        return;
    }
}

void INTERPRETER::DELETE()
{
    table_name = token_Buffer[2];

    // for(int i = 0; i < token_Buffer.size(); i++)
    // {
    //     std::cout<<token_Buffer[i]<<std::endl;
    // }

    std::string column = token_Buffer[4];
    std::string column_value = token_Buffer[6];

    column_value.erase(column_value.length() - 1, column_value.length());


    int col_index = 0;

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);

        getline(dataFile, record);

        std::stringstream check1(record);

        token_Buffer.clear();

        while(getline(check1, token, '#'))
        {
            token_Buffer.push_back(token);
        }

        bool c = false;
        for(int i = 0; i < token_Buffer.size(); i++)
        {
            if(token_Buffer[i] == column)
            {
                col_index = i;
                c = true;
            }
        }

        if(!c)
        {
            std::cout<<"ERROR: CONDITION COLUMN DOES NOT EXIST FOR TABLE!"<<std::endl<<std::endl;
            return;
        }

        dataFile.close();
        dataFile.open(table_name, std::ios::in);

        std::fstream dataFile_new;
        dataFile_new.open(table_name + "_new", std::ios::out);

        while(!dataFile.eof())
        {
            getline(dataFile, record);

            std::stringstream check2(record);

            record_vector.clear();
            while(getline(check2, token, '#'))
            {
                record_vector.push_back(token);
            }

            if(record_vector[col_index] == column_value || record == "" || record == "\n")
                continue;

            dataFile_new << record<<std::endl;;
        }

        dataFile_new.close();
        dataFile.close();

        remove(table_name.c_str());

        rename((table_name + "_new").c_str(), table_name.c_str());

        std::cout<<"RECORD DELETED!"<<std::endl<<std::endl;

    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
    }
}

void INTERPRETER::UPDATE()
{   
    table_name = token_Buffer[1];

    pk_column_name = "NULL";

    std::string col_name = token_Buffer[3];

    std::string new_value = token_Buffer[5];

    std::string condition_column = token_Buffer[token_Buffer.size() - 3];
    
    std::string condition_column_value = token_Buffer[token_Buffer.size() - 1];
    condition_column_value.erase(condition_column_value.length() - 1, condition_column_value.length());

    std::fstream dataFile(table_name, std::ios::in);

    if(dataFile)
    {
        dataFile.close();
        dataFile.open(table_name, std::ios::in);
    }
    else
    {
        std::cout<<"ERROR: NO SUCH TABLE!"<<std::endl<<std::endl;
        return;
    }

    schema.close();
    schema.open("SCHEMA.txt", std::ios::in);

    while(!schema.eof())
    {
        getline(schema, record);

        record_vector.clear();

        std::stringstream check1(record);

        while(getline(check1, token, '#'))
        {
            record_vector.push_back(token);
        }

        if(record_vector[0] == table_name)
        {
            if(record_vector[1] == "PRIMARY KEY")
                pk_column_name = record_vector[2];
        }
    }

    getline(dataFile, record);

    if(pk_column_name != "NULL")
    {
        record_vector.clear();

        std::stringstream check2(record);

        while(getline(check2, token, '#'))
        {
            record_vector.push_back(token);
        }

        for(int i = 0; i < record_vector.size(); i++)
        {
            if(record_vector[i] == pk_column_name)
                primary_key_column = i;
        }
    }

    record_vector.clear();

    std::fstream dataFile_new(table_name + "_new", std::ios::app);

    token_Buffer.clear();

    int condition_column_index;
    int col_index;

    std::stringstream check2(record);

    record_vector.clear();

    while(getline(check2, token, '#'))
    {
        record_vector.push_back(token);
    }

    bool c1 = false;
    bool c2 = false;
    for(int i = 0; i < record_vector.size(); i++)
    {
        if(record_vector[i] == condition_column)
        {
            condition_column_index = i;
            c1 = true;
        }     

        if(record_vector[i] == col_name)
        {
            col_index = i;
            c2 = true;
        }
    }

    if(!c1)
    {
        std::cout<<"ERROR: CONDITION COLUMN DOES NOT EXIST FOR TABLE!"<<std::endl<<std::endl;
        return;
    }

    if(!c2)
    {
        std::cout<<"ERROR: COLUMN DOES NOT EXIST FOR TABLE!"<<std::endl<<std::endl;
        return;
    }

    record_vector.clear();
    std::queue<int> line_numbers;
    int line = 1;
    int lines = 0;
    while(!dataFile.eof())
    {
        line++;
        getline(dataFile, record);

        std::stringstream check1(record);

        while(getline(check1, token, '#'))
        {
            record_vector.push_back(token);
        }

        if(record_vector[condition_column_index] == condition_column_value)
        {
            line_numbers.push(line);
            lines++;
        }

        record_vector.clear();
    }


    record_vector.clear();

    dataFile.close();

    int check_line = 0;
    dataFile.open(table_name, std::ios::in);

    if(col_index == primary_key_column && primary_key_column != -1)
    {
        while(!dataFile.eof())
        {
            getline(dataFile, record);

            record_vector.clear();

            std::stringstream check3(record);

            while(getline(check3, token, '#'))
            {
                record_vector.push_back(token);
            }

            if(record_vector[primary_key_column] == new_value)
            {
                std::cout<<"ERROR: PRIMARY KEY MUST BE UNIQUE!"<<std::endl;
                return;
            }
        }
    }

    dataFile.close();
    dataFile.open(table_name, std::ios::in);

    while(!dataFile.eof())
    {
        check_line++;

        getline(dataFile, record);

        if(check_line == line_numbers.front() && !line_numbers.empty())
        {
            line_numbers.pop();
            std::stringstream check1(record);

            while(getline(check1, token, '#'))
            {
                record_vector.push_back(token);
            }

            record_vector[col_index] = new_value;

            std::string new_record = "";
            for(int i = 0; i < record_vector.size(); i++)
            {
                new_record += record_vector[i] + "#";
            }

            if(new_record == "")
                continue;

            dataFile_new<<new_record<<std::endl;

            record_vector.clear();
        }
        else
        {
            if(record == "")
                continue;
            dataFile_new<<record<<std::endl;
        }
    }

    dataFile_new.close();
    dataFile.close();

    remove(table_name.c_str());

    rename((table_name + "_new").c_str(), table_name.c_str());

    // if(lines > 1)
    //     std::cout<<"\n"<<lines - 1<<" ROWS AFFECTED!"<<std::endl<<std::endl;
    // else
    //    std::cout<<"\n"<<lines<<" ROW AFFECTED!"<<std::endl<<std::endl;

    std::cout<<"\nRECORD(s) UPDATED!"<<std::endl<<std::endl;

}

void INTERPRETER:: HELP_TABLES(){
  std::string tab_name;
  mytables.open("tables.txt");
  while(!mytables.eof()){ 
		mytables>>tab_name; 
		std::cout<< tab_name<< std::endl; 
	} 
    mytables.close();
}

int main()
{
    INTERPRETER i;

    std::string query;

    do
    {
        std::cout<<"SQL > ";
        getline(std::cin, query);
        i.LOAD_QUERY(query);
    } while (query != "QUIT;");
    
}