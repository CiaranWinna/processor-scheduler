//package CC;

import java.util.Arrays;
import java.lang.reflect.Array;
import java.util.*;

public class CC {

    /**
     * Notes: - Execute all given transactions, using locking. - Each element in the
     * transactions List represents all operations performed by one transaction, in
     * order. - No operation in a transaction can be executed out of order, but
     * operations may be interleaved with other transactions if allowed by the
     * locking. - The index of the transaction in the list is equivalent to the
     * transaction ID. - Print the log to the console at the end of the method. -
     * Return the new db state after executing the transactions.
     * 
     * @param db           the initial status of the db
     * @param transactions the schedule, which basically is a {@link List} of
     *                     transactions.
     * @return the final status of the db
     */
    private static Shared_locks shared_locks;
    private static Exclusive_locks exclusive_locks;
    private static Log logs;
    private static int number_of_transactions = 0;
    private static int progress_char_of_transactions[];
    private static int round_robin = 0;
    private static boolean transactions_finsihed = false;
    private static boolean commit[];
    private static Node nodes[];
    private static int commits = 0;
    private static boolean deadlock_check = false;
    private static boolean deadlock_detected = false;
    private static ArrayList<Integer> deadlocks_list;
    private static boolean is_aborted[];

    public int[] executeSchedule(int[] db, List<String> transactions) {

        // number of transactions
        number_of_transactions = transactions.size();

        // initializing commit checker array
        commit = new boolean[number_of_transactions];

        // initializing the shared lock array list
        shared_locks = new Shared_locks(db.length);

        // initializing the exlusive locks
        exclusive_locks = new Exclusive_locks(db.length);

        progress_char_of_transactions = new int[number_of_transactions];
        // initializing the progress array
        for (int i = 0; i < progress_char_of_transactions.length; i++) {
            progress_char_of_transactions[i] = 0;
        }

        // initialize the log arrayList
        logs = new Log(number_of_transactions);

        // initialize the aborted array

        is_aborted = new boolean[number_of_transactions];
        for (int i = 0; i < is_aborted.length; i++) {
            is_aborted[i] = false;
        }

        // adding nodes to the wait-for-graph
        nodes = new Node[number_of_transactions];
        for (int i = 0; i < number_of_transactions; i++) {
            nodes[i] = new Node(i);
        }

        while (!transactions_finsihed) {

            if (round_robin < number_of_transactions) {
                // getting current transaction, element to update and value to update element in
                // db
                String transaction = transactions.get(round_robin);
                int element;
                int value;
                char operation = transaction.charAt(progress_char_of_transactions[round_robin]);

                System.out.println("\nCurrent transaction: " + transaction);

                // checking for deadlocks

                // would have been used for deadlocks
                /*
                 * while (deadlock_check == true) { fix_deadlocks(); }
                 */

                // the if statement would of been used for deadlocks
                // if (is_aborted[round_robin] == false) {

                // transaction is a write
                if (operation == 'W') {
                    // getting transaction information
                    element = (transaction.charAt(progress_char_of_transactions[round_robin] + 2)) - '0';
                    value = (transaction.charAt(progress_char_of_transactions[round_robin] + 4)) - '0';

                    // getting locking information
                    Integer current_ex_lock_holder = exclusive_locks.get_current_lock_holder(element);
                    int shared_lock_number_for_element = shared_locks.number_of_locks_for_element(element);
                    boolean is_present = shared_locks.is_transaction_present_in_locks(element, round_robin);

                    // if transaction is granted the exclusive lock
                    if ((current_ex_lock_holder == null || current_ex_lock_holder.equals(round_robin))
                            && ((shared_lock_number_for_element == 0)
                                    || ((is_present == true) && (shared_lock_number_for_element == 1)))) {

                        // get exclusive lock for the element
                        exclusive_locks.set_lock(element, round_robin);
                        // checking if transaction was granted exclusive lock
                        int is_granted = exclusive_locks.get_current_lock_holder(element);

                        // remove shared lock if it exists
                        boolean is_removed = shared_locks.remove_lock(element, round_robin);

                        // update the element
                        if ((is_removed == true) && (is_granted == round_robin)) {
                            // storing value before it is updated
                            int temp_value = db[element];

                            // updating value of database
                            db[element] = value;

                            // move the pointer for the current transaction
                            progress_char_of_transactions[round_robin] += 7;
                            System.out.println("Current operation: " + operation + " | Next operation for transaction: "
                                    + transaction.charAt(progress_char_of_transactions[round_robin]));

                            System.out.println("UPADATE SUCCESSFUL");

                            // adding a log to the log array
                            logs.add_write(operation, round_robin, element, temp_value, value);
                        }

                    }
                    // if there is an exclusive lock held by another transaction
                    else {
                        System.out.println("UPDATE UNSUCCESSFUL, exclusive lock held by another transaction");

                        // This piece of code would have been used for deadlocks
                        /*
                         * getting current exclusive lock holder for current element int
                         * current_lock_holder_ex = exclusive_locks.get_current_lock_holder(element); //
                         * adding an adjacent to the current elements which is being blocked with the //
                         * current lock holder
                         * nodes[round_robin].addAdjacent(nodes[current_lock_holder_ex]); Boolean
                         * is_added = nodes[round_robin].addAdjacent(nodes[current_lock_holder_ex]); if
                         * (is_added == true) { System.out.println("Node:" + nodes[round_robin] +
                         * " added edge to node " + nodes[current_lock_holder_ex] + " (Shared Lock)"); }
                         * else if (is_added == true) { System.out.println("Node:" + nodes[round_robin]
                         * + " already added edge to node " + nodes[current_lock_holder_ex] +
                         * " (Exclusive Lock)"); } deadlock_check = true;
                         */

                    }

                    // if the next action operation in the transaction is a read
                } else if (transaction.charAt(progress_char_of_transactions[round_robin]) == 'R') {
                    element = (transaction.charAt(progress_char_of_transactions[round_robin] + 2)) - '0';

                    // checking if correct data is recorded
                    System.out.println("Current operation: " + operation + " Current element: " + element);

                    // checking if there is an exclusive lock on the element
                    Integer exclusive_lock_taken = exclusive_locks.get_current_lock_holder(element);
                    boolean shared_lock_already_acquired = shared_locks.is_transaction_present_in_locks(element,
                            round_robin);
                    System.out.println("Exclusive lock held by transaction: " + exclusive_lock_taken
                            + " | Shared lock already held by current transaction: " + shared_lock_already_acquired);

                    if ((exclusive_lock_taken == null) || (exclusive_lock_taken == round_robin)) {
                        // granting transaction a shared key
                        if (shared_lock_already_acquired == false) {
                            boolean lock_shared = shared_locks.add_lock(element, round_robin);
                            if (lock_shared) {
                                System.out.println("Transaction " + (round_robin + 1) + " granted shared key");
                            }
                        }
                        System.out.println("READ SUCCESSFUL");

                        System.out.println("Current pointer: " + progress_char_of_transactions[round_robin]);
                        // incrementing the pointer for the current transaction
                        progress_char_of_transactions[round_robin] += 5;
                        System.out.println("Next pointer: " + progress_char_of_transactions[round_robin]);

                        // print current and next operation of current transaction
                        System.out.println("Current operation: " + operation + " | Next operation for transaction: "
                                + transaction.charAt(progress_char_of_transactions[round_robin]));
                        // adding a log to the log array
                        logs.add_read(operation, round_robin, element, db[element]);

                    } else {
                        System.out.println("READ UNSUCCESSFUL");
                        System.out.println("Current pointer: " + progress_char_of_transactions[round_robin]);

                        // Would have been used for deadlocks
                        /*
                         * int current_lock_holder_ex =
                         * exclusive_locks.get_current_lock_holder(element); Boolean is_added =
                         * nodes[round_robin].addAdjacent(nodes[current_lock_holder_ex]); if (is_added
                         * == true) { System.out.println("Node:" + nodes[round_robin] +
                         * " added edge to node " + nodes[current_lock_holder_ex] + " (Shared Lock)"); }
                         * else if (is_added == false) { System.out.println("Node:" + nodes[round_robin]
                         * + " already added edge to node " + nodes[current_lock_holder_ex] +
                         * " (Shared Lock)"); } deadlock_check = true;
                         */

                    }

                    // if the next operation in the transaction is a commit operation
                } else if (transaction.charAt(progress_char_of_transactions[round_robin]) == 'C') {
                    if (commit[round_robin] == false) {
                        System.out.println("Current operation:" + operation);
                        boolean ex_locks_removes = exclusive_locks.remove_locks((Integer) round_robin);
                        boolean sh_locks_removed = shared_locks.remove_locks(round_robin);
                        // checking if all exclusive locks held by transaction were removed
                        if (ex_locks_removes && sh_locks_removed) {
                            System.out.println("Exclusive and Shared locks held have been removed");
                        } else {
                            System.out.println("Exclusive and shared locks have not deleted");
                        }
                        commit[round_robin] = true;
                        commits++;
                        // adding to log array
                        logs.add_commit(operation, round_robin);
                    } else {
                        System.out.println("Transaction has already committed");
                    }
                } else {
                    // increments due to pointer being over a semi colon //
                    progress_char_of_transactions[round_robin] += 1;
                }
                // }

                // printing current transaction
                System.out.println("Current Transaction: " + (round_robin + 1));

                // Would have been used for deadlocks
                /*
                 * if (deadlock_detected == true) { System.out.
                 * println("Due to R/W unsuccessful, deadlock will be checked in next round robin"
                 * ); } else { round_robin++; }
                 */

                // incrementing round robin for next transaction
                round_robin++;

                // printing next transaction
                if (round_robin >= number_of_transactions) {
                    System.out.println("Next Transaction: 1");
                } else {
                    System.out.println("Next Transaction: " + (round_robin + 1));
                }
                // printing extra information about the current state of the transactions
                System.out.println("Exclusive array size: " + exclusive_locks.size());
                exclusive_locks.print();
                System.out.println("Shared array size: " + shared_locks.size());
                shared_locks.print();

            } else {
                // reset round robin round_robin = 0;
                round_robin = 0;
                System.out.println("\nROUND ROBIN RESET! Value: " + round_robin + "\n");
            }

            // checking if commits match number of transactions
            if (commits >= number_of_transactions) {
                transactions_finsihed = true;

                System.out.println("\n\nTransaction has completed!\n\n");
            }
        }

        // print the transaction log to the screen
        logs.print_transaction_log();

        // TODO
        return db;
    }

    private static class Exclusive_locks {
        private Integer[] ex_locks;

        private Exclusive_locks(int size) {
            ex_locks = new Integer[size];
        }

        private Integer get_current_lock_holder(int el) {
            return ex_locks[el];
        }

        private void set_lock(int el, int tra) {
            ex_locks[el] = tra;
        }

        private void print() {
            System.out.println("Exclusive lock array: " + Arrays.toString(ex_locks));
        }

        private int size() {
            return ex_locks.length;
        }

        private boolean remove_locks(int tra) {
            System.out.println("Sent transaction for deletion of exclusive locks: " + tra);
            for (int i = 0; i < ex_locks.length; i++) {
                if (ex_locks[i] == (Integer) tra) {
                    ex_locks[i] = null;
                }
            }
            return true;
        }
    }

    private static class Shared_locks {
        private List<ArrayList<Integer>> sh_locks;

        private Shared_locks(int size) {
            sh_locks = new ArrayList<ArrayList<Integer>>();

            for (int i = 0; i < size; i++) {
                sh_locks.add(new ArrayList<Integer>());
            }
        }

        private int number_of_locks_for_element(int el) {
            return sh_locks.get(el).size();
        }

        private boolean add_lock(int el, int tra) {
            if (sh_locks.get(el).contains(tra)) {
                return true;
            } else if (sh_locks.get(el).add(tra)) {
                return true;
            } else {
                return false;
            }
        }

        // if not a commit operation
        private boolean remove_lock(int el, int tra) {
            System.out.println("Delete shared lock | Element: " + el + " | transaction: " + tra);
            // does the array contain the transaction key
            if (sh_locks.get(el).contains(tra)) {
                System.out.println("Shared array does contain the transaction's key!");
                // if its the only key in the array
                if (sh_locks.get(el).size() <= 1) {
                    sh_locks.get(el).remove(0);
                }
                // if there's more than one key in the array
                else {
                    sh_locks.get(el).remove(tra);
                }
                return true;
            } else {
                return true;
            }
        }

        private boolean remove_locks(int tra) {
            for (ArrayList<Integer> array : sh_locks) {
                // loop through sub arrays and remove all transaction's locks
                if (array.contains(tra)) {
                    array.removeAll(Collections.singletonList(tra));
                }
            }
            return true;
        }

        // check if current transaction has a shared lock for the element it wishes to
        // access
        private boolean is_transaction_present_in_locks(int el, int tra) {
            boolean is_present = sh_locks.get(el).contains(tra);
            return is_present;
        }

        private void print() {
            System.out.println("Shared array:");
            for (int i = 0; i < sh_locks.size(); i++) {
                System.out.println(sh_locks.get(i));
            }
        }

        private int size() {
            return sh_locks.size();
        }
    }

    private static class Log {

        // private variables
        private List<String[]> logs_array;
        private int counter = 0;
        private String[] previous_entry_pointer;

        private Log(int num_of_trans) {
            // initializing elements of class
            logs_array = new ArrayList<String[]>();
            previous_entry_pointer = new String[num_of_trans];
            for (int i = 0; i < previous_entry_pointer.length; i++) {
                previous_entry_pointer[i] = "-1";
            }

        }

        // add method for the log array if its a write
        private boolean add_write(char oper, int tra, int rec, int ov, int nv) {
            try {
                // initialing the log array for the current log entry of the passed transaction
                String[] write_array = new String[7];
                // operation
                write_array[0] = String.valueOf(oper);
                // timestamp
                write_array[1] = String.valueOf(counter);
                // transaction id
                write_array[2] = String.valueOf("T" + (tra + 1));
                // record id
                write_array[3] = String.valueOf(rec);
                // old value
                write_array[4] = String.valueOf(ov);
                // new value
                write_array[5] = String.valueOf(nv);
                // previous pointer
                write_array[6] = previous_entry_pointer[tra];

                // add log into logs arrayList
                if (this.logs_array.add(write_array)) {
                    // increment previous entry for the current transaction
                    previous_entry_pointer[tra] = String.valueOf(counter);
                    // increment the counter
                    counter++;

                    // printing to the console
                    System.out.println("WRITE LOG ADDED!");
                    // return true;
                    return true;

                } else {
                    System.out.println("WRITE LOG NOT ADDED!");
                    return false;
                }
            } catch (Exception e) {
                System.out.println("EXCEPTION: " + e);
                return false;
            }
        }

        // add method for the log array if its a read
        private boolean add_read(char oper, int tra, int rec, int vr) {
            try {
                String[] read_array = new String[6];
                // operation
                read_array[0] = String.valueOf(oper);
                // timestamp
                read_array[1] = String.valueOf(counter);
                // transaction id
                read_array[2] = String.valueOf("T" + (tra + 1));
                // record id
                read_array[3] = String.valueOf(rec);
                // value read from database
                read_array[4] = String.valueOf(vr);
                // previous pointer for transaction
                read_array[5] = previous_entry_pointer[tra];

                // add log into logs arrayList
                if (this.logs_array.add(read_array)) {
                    // increment previous entry for the current transaction
                    previous_entry_pointer[tra] = String.valueOf(counter);
                    // increment the counter
                    counter++;

                    // printing to the console
                    System.out.println("READ LOG ADDED!");
                    // return true;
                    return true;
                } else {
                    System.out.println("READ LOG NOT ADDED!");
                    return false;
                }
            } catch (Exception e) {
                System.out.println("EXCEPTION: " + e);
                return false;
            }
        }

        // add method for the log array if its a commit
        private boolean add_commit(char oper, int tra) {
            try {
                String[] commit_array = new String[4];
                // operation
                commit_array[0] = String.valueOf(oper);
                // timestamp
                commit_array[1] = String.valueOf(counter);
                // transaction id
                commit_array[2] = String.valueOf("T" + (tra + 1));
                // pointer of last operation of transaction
                commit_array[3] = previous_entry_pointer[tra];

                // add log into logs arrayList
                if (this.logs_array.add(commit_array)) {
                    // increment previous entry for the current transaction
                    previous_entry_pointer[tra] = String.valueOf(counter);
                    // increment the counter
                    counter++;

                    // printing to the console
                    System.out.println("COMMIT LOG ADDED!");
                    // return true;
                    return true;
                } else {
                    System.out.println("COMMIT LOG NOT ADDED!");
                    return false;
                }
            } catch (Exception e) {
                System.out.println("EXCEPTION: " + e);
                // return false
                return false;
            }
        }

        private void add_abort(int tra) {
            String[] abort_array = new String[4];
            // operation
            abort_array[0] = "A";
            // timestamp
            abort_array[1] = String.valueOf(counter);
            // transaction id
            abort_array[2] = String.valueOf("T" + (tra + 1));
            // pointer of last operation of transaction
            abort_array[3] = previous_entry_pointer[tra];

        }

        private void print_transaction_log() {
            System.out.println("Log of transaction:");
            for (int i = 0; i < logs_array.size(); i++) {
                String record = Arrays.toString(logs_array.get(i));
                System.out.println(record);
            }
            System.out.println("");
        }
    }

    // ---------------------------------------------------//
    // Classes that deal with deadlocks //
    // --------------------------------------------------//

    private class Node {
        private int val;
        private boolean visited;
        private List<Node> adjacents;

        public Node(int val) {
            this.val = val;
            this.adjacents = new ArrayList<>();
        }

        public Boolean addAdjacent(Node n) {
            if (!adjacents.contains(n)) {
                this.adjacents.add(n);
                return true;
            } else {
                return true;
            }
        }

        public List<Node> getAdjacenets() {
            return adjacents;
        }

        public int getVal() {
            return this.val;
        }

        public boolean isVisited() {
            return this.visited;
        }

        public void setVal(int v) {
            this.val = v;
        }

        public void setVisited(boolean visited) {
            this.visited = visited;
        }

        public void removeAdjacents() {
            adjacents.clear();
        }

        public void remove_aborted_adjacents(int tra) {
            for (int i = 0; i < adjacents.size(); i++) {
                if (adjacents.get(i).getVal() == tra) {
                    adjacents.remove(i);
                }
            }
        }
    }

    private class DFS {
        private ArrayList<Integer> deadlocks;

        public void stackSolution(Node node) {
            deadlocks = new ArrayList<Integer>();
            Stack<Node> DFS_stack = new Stack<Node>();
            DFS_stack.add(node);
            node.setVisited(true);
            while (!DFS_stack.isEmpty()) {
                Node nodeRemove = DFS_stack.pop();
                System.out.print(nodeRemove.getVal() + " ");

                List<Node> adjs = nodeRemove.getAdjacenets();
                for (int i = 0; i < adjs.size(); i++) {
                    Node currentNode = adjs.get(i);
                    if (currentNode != null && !currentNode.isVisited()) {
                        DFS_stack.add(currentNode);
                        currentNode.setVisited(true);
                    } else if (currentNode != null && currentNode.isVisited() == true) {
                        System.out.println("\n\nThis transaction is in a deadlock");
                        deadlocks.add(currentNode.getVal());
                        for (int j = 0; j < currentNode.adjacents.size(); j++) {
                            if (currentNode.adjacents.get(j).visited == true) {
                                deadlocks.add(currentNode.adjacents.get(j).getVal());
                            }
                        }
                        System.out.print(Arrays.toString(deadlocks.toArray()));
                    }
                }
            }
        }

        private ArrayList<Integer> getDeadLocks() {
            return deadlocks;
        }

        private boolean isDeadlocked() {
            if (deadlocks.isEmpty() == true) {
                return false;
            } else {
                return true;
            }
        }

    }

    private boolean fix_deadlocks() {
        deadlocks_list = new ArrayList<Integer>();
        for (int i = 0; i < number_of_transactions; i++) {
            DFS search = new DFS();
            search.stackSolution(nodes[i]);
            if (search.isDeadlocked()) {
                deadlocks_list.addAll(search.getDeadLocks());
                deadlock_detected = true;
            }
        }

        if (deadlocks_list.isEmpty() == false) {
            System.out.println("Current Deadlock List:\n" + (Arrays.toString(deadlocks_list.toArray())));
            int highest_transaction = 0;
            for (int i = 0; i < deadlocks_list.size(); i++) {
                if (deadlocks_list.get(i) > highest_transaction) {
                    highest_transaction = deadlocks_list.get(i);
                }
            }
            // setting the transaction to be aborted
            is_aborted[highest_transaction] = true;
            // removing any exclusive locks that the transaction has
            exclusive_locks.remove_locks(highest_transaction);
            // removing the transaction from the shared locks
            shared_locks.remove_locks(highest_transaction);
            // removing transaction from the node array
            nodes[highest_transaction].removeAdjacents();
            // write the log
            logs.add_abort(highest_transaction);
            // remove aborted transaction from other nodes
            for (int i = 0; i < nodes.length; i++) {
                if (i != highest_transaction) {
                    nodes[i].remove_aborted_adjacents(highest_transaction);
                }
            }
            return false;

        } else {
            System.out.println("No deadlocks detected!");
            deadlock_detected = false;
            deadlock_check = false;
            return true;
        }
    }
}
