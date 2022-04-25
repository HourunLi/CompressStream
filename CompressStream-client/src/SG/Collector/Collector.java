import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;
import Utils.*;

public class Collector implements Runnable{
    public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW = Definitions.TUPLE_NUM_PER_WINDOW;
    public final static int WINDOW_NUM_PER_BATCH = Definitions.WINDOW_NUM_PER_BATCH;//
    public final static int BATCH_NUM = Definitions.BATCH_NUM;
    public final static String DATA_DIR = Definitions.DATA_DIR;
    public final static String DATA_FILE = Definitions.DATA_FILE;
    public final static int START_BATCH = Definitions.START_BATCH;
    public final static int END_BATCH = Definitions.END_BATCH;

    public final static int pow7= 128, pow8 = 256, pow15 = 32768, pow16 = 65536, pow23=8388608, pow24 = 16777216, pow31 = 2147483647;
    public final static int EG1 = 14, EG2 = 254, EG3 = 4094, EG4 = 65534;
    public final static int ED1 = 14, ED2 = 1022, ED3 = 65534, ED4 = 16777214;

    private String name;

    public Collector(String name) {
        this.name = name;
        this.name = "";
    }

    @Override
    public void run() {
        try {
            process(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void process(String name) throws Exception {
        FileInputStream f1;
        DataInputStream d1;
        BufferedReader b1;
        f1 = new FileInputStream(DATA_DIR+DATA_FILE+name+".csv");
        d1 = new DataInputStream(f1);
        b1 = new BufferedReader(new InputStreamReader(d1));

        int count = 0;
        int batchcount = 0;

        String line1;

        int[] base = new int[7];
        int[] last = new int[7];
        int[] EGDomain = new int[7];
        int[] EDDomain = new int[7];
        int[][] ValueDomain = new int[7][TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        int[] ValueDomainMax = new int[7];
        int[] BDDomain = new int[7];
        int[] AverageRunLength = new int[7];
        int[] NumDistinct = new int[7];
        int[][] dict = new int[7][TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        int[] dict_count = new int[7];
        int[] element_count = {1, 1, 1, 1, 1, 1, 1};

        // for(int j = 0; j < NumDistinct; j++) {
        //     dict[j] = -1;
        // }
        // int dict_count = 0;
        // int[] result = new int[col.length + NumDistinct];
        // int col_count = 0;
        // for(int i = 0; i < col.length; i++) {
        //     int flag = 0;
        //     for(int j = 0; j < dict_count; j++) {
        //         if(dict[j] == col[i]) {
        //             result[col_count++] = j;
        //             flag = 1;
        //             break;
        //         }
        //     }
        //     if(flag == 0) {
        //         result[col_count++] = dict_count;
        //         dict[dict_count++] = col[i];    
        //     }
        // }

        while ((line1 = b1.readLine()) != null) {
            // System.out.println(line1);

            String[] split = line1.split(",");

            if(split.length<7)
              continue;
            if(count == 0) {
                for(int i = 1; i < 7; i++) {
                    if(i != 2) {
                        base[i] = Integer.parseInt(split[i]);
                        last[i] = Integer.parseInt(split[i]);
                    }
                }
                batchcount ++;
            }
            if (count < TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) {
                for(int i = 1; i < 7; i++) {
                    if(i != 2) {
                        int num = Integer.parseInt(split[i]);
                        //EGDomain
                        if(num < 0) {
                            EGDomain[i] = 5;
                        }
                        else {
                            int temp = 0;
                            if(num < EG1) {
                                temp = 1;
                            }
                            else if(num < EG2) {
                                temp = 2;
                            }
                            else if(num < EG3) {
                                temp = 3;
                            }
                            else if(num < EG4) {
                                temp = 4;
                            }
                            else
                                temp = 5;
                            EGDomain[i] = Math.max(temp, EGDomain[i]);
                        }
                        //EDDomain
                        if(num < 0) {
                            EDDomain[i] = 5;
                        }
                        else {
                            int temp = 0;
                            if(num < ED1) {
                                temp = 1;
                            }
                            else if(num < ED2) {
                                temp = 2;
                            }
                            else if(num < ED3) {
                                temp = 3;
                            }
                            else if(num < ED4) {
                                temp = 4;
                            }
                            else
                                temp = 5;
                            EDDomain[i] = Math.max(temp, EDDomain[i]);
                        }
                        //ValueDomain
                        if(num > -pow7 && num < pow7) {
                            ValueDomain[i][count] = 1;
                        }
                        else if(num > -pow15 && num < pow15) {
                            ValueDomain[i][count] = 2;
                        }
                        else if(num > -pow23 && num < pow23) {
                            ValueDomain[i][count] = 3;
                        }
                        else if(num > -pow31 && num < pow31) {
                            ValueDomain[i][count] = 4;
                        }
                        else
                            ValueDomain[i][count] = 5;
                        // ValueDomainMax
                        ValueDomainMax[i] = Math.max(ValueDomainMax[i], ValueDomain[i][count]);

                        // NumDistinct
                        int flag = 0;
                        for(int j = 0; j < dict_count[i]; j++) {
                            if(dict[i][j] == num) {
                                flag = 1;
                                break;
                            }
                        }
                        if(flag == 0) {
                            dict[i][dict_count[i]++] = num;    
                        }
                        NumDistinct[i] = Math.max(NumDistinct[i], dict_count[i]);

                        // AverageRunLength
                        if(num == last[i]) {
                            AverageRunLength[i]++;
                        }
                        else {
                            last[i] = num;
                            element_count[i] ++;
                        }

                        // BDDomain
                        num = num - base[i];
                        int delta_tmp = 0;
                        if(num > -pow7 && num < pow7) {
                            delta_tmp = 1;
                        }
                        else if(num > -pow15 && num < pow15) {
                            delta_tmp = 2;
                        }
                        else if(num > -pow23 && num < pow23) {
                            delta_tmp = 3;
                        }
                        else if(num > -pow31 && num < pow31) {
                            delta_tmp = 4;
                        }
                        else
                            delta_tmp = 5;
                        BDDomain[i] = Math.max(BDDomain[i], delta_tmp);

                    }
                }
                count += 1;
            }
            if (count == TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) {
                count = 0;
                dict = new int[7][TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
                dict_count = new int[7];
                // element_count = new int[7];
                if(batchcount == 200) {
                    System.out.print("NumDistinct: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(NumDistinct[i] + " ");
                    }
                    System.out.println();
                    System.out.print("EDDomain: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(EDDomain[i] + " ");
                    }
                    System.out.println();
                    System.out.print("EGDomain: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(EGDomain[i] + " ");
                    }
                    System.out.println();

                    System.out.print("ValueDomainMax: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(ValueDomainMax[i] + " ");
                    }
                    System.out.println();
                    System.out.print("BDDomain: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(BDDomain[i] + " ");
                    }
                    System.out.println();
                    System.out.print("AverageRunLength: ");
                    for(int i = 1; i < 7; i++) {
                        System.out.print(AverageRunLength[i]/element_count[i] + " ");
                    }
                    System.out.println();
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        new Thread(new Collector("1")).start();

    }
}
