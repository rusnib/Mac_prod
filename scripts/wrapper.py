import sys

#запуск: python3 [путь к этому скрипту] [полный путь к файлу] <rw>
# rw -> rewrite (перезапись текущего файла)

FILE = sys.argv[1]

isRewrite = len(sys.argv) > 2 and sys.argv[2] == 'rw'
FILE_RESULT = FILE if not isRewrite else (FILE[:FILE.index('.sas')] + '_w.sas')

BANNED_PROCS = [
    'casutil'
]

def remove_semicolon(name: str) -> str:
    return name[:-1] if ';' in name else name


tech_job_det = '%tech_job_details(mpMODE={}, mpSTEP_TYPE={})\n'

with open(FILE, 'r') as file_input:
    lines = file_input.readlines()
    with open(FILE_RESULT, 'w') as file_output:
        last_step_type = ''
        prev_line = ''
        isBanned = False
        for line in lines:
            words = line.split()
            if words and words[0].lower() == '%macro' and isRewrite:
                name = words[1]
                name = name[:name.index('(')]
                if len(name) <= 30:
                    new_name = name + '_w'
                    args = words[1]
                    args = args[args.index('('):]
                    file_output.write("%macro " + new_name + args + '\n')
                else:
                    raise NameError('New macro name have length more 32')
            elif words and words[0].lower() == '%mend' and isRewrite:
                name = remove_semicolon(words[1])
                new_name = name + '_w'
                file_output.write('%macro ' + new_name + ';\n')
            elif words and words[0].lower() in ('data', 'proc'):
                isBanned = words[0].lower() == 'proc' and remove_semicolon(words[1].lower()) in BANNED_PROCS
                if not isBanned:
                    last_step_type = words[0].lower()
                    file_output.write('\t'*line.count('\t') + tech_job_det.format('START', last_step_type))
                file_output.write(line)
            elif words and remove_semicolon(words[0].lower()) in ('run', 'quit'):
                file_output.write(line)
                if prev_words and remove_semicolon(prev_words[0].lower()) in ('run', 'quit'):
                    if isBanned:
                        isBanned = False
                    else:
                        file_output.write('\t'*line.count('\t') + tech_job_det.format('END', last_step_type))
                        last_step_type = ''
            else:
                if last_step_type != '':
                    if prev_words and remove_semicolon(prev_words[0].lower()) in ('run', 'quit'):
                        if isBanned:
                            isBanned = False
                        else:
                            file_output.write('\t'*prev_line.count('\t') + tech_job_det.format('END', last_step_type))
                            last_step_type = ''
                file_output.write(line)
            prev_line = line
            prev_words = words



