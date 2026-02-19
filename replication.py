import datetime

from primary import Primary
from secondary import Secondary


class Replication:
    def __init__(self, primary: Primary, secondary: Secondary):
        self.primary = primary
        self.secondary = secondary

    def run(self, name, today: str):
        subscription_name = self.secondary.get_subscription_name(self.primary.db.db_name)

        # Check if replication is already started
        if subscription_name == "":
            print("Replication not in progress")
            print(f"{today} - Starting process : {name} {self.primary.db.conn_string} {self.primary.db.db_name} - {self.secondary.db.conn_string} database {self.secondary.db.db_name}")
    
            self.primary.create_replication_user()
    
            # Section pre-data
            self.run_dump_restore_pre()
    
            # Section post-data
            self.run_dump_restore_post_only_pk()
    
            date_start = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_name = f"{self.primary.db.db_name}_{date_start}"
    
            self.primary.create_publication(unique_name)
    
            self.secondary.create_subscription(unique_name)
    
            subscription_name = self.secondary.get_subscription_name(self.primary.db.db_name)
    
        if subscription_name:
            print(
                f"Check if first step of replication is done - db {self.secondary.db.db_name} on host {self.secondary.db.conn_string} from {self.primary.db.conn_string} database {self.primary.db.db_name}")
            self.secondary.wait_first_step_of_replication()
    
            # Disable subscription
            self.secondary.disable_subscription(subscription_name)
    
            # Restore post section without primary keys
            print("Restore post section - without primary key")
            self.run_dump_restore_post_without_pk()
    
            # Enable subscription
            self.secondary.enable_subscription(subscription_name)
    
            end_time = datetime.datetime.now().strftime("%Y%m%d-%H-%M-%S")
            print(f"end={end_time}")
        else:
            print("No replication running, exiting")
    
        print("end")

    def run_dump_restore_pre(self):
        print(f"pg_restore pre begin")
    
        with self.primary.execute_dump("pre-data") as dump:
            self.secondary.execute_pre_data_dump(dump)
    
        print(f"run_dump_restore_pre end")
    
    
    def run_dump_restore_post_only_pk(self):
        print(f"pg_restore post begin")
    
        with self.primary.execute_dump("post-data") as dump:
            self.secondary.execute_post_data_dump_only_pk(dump)
    
        print(f"run_dump_restore_post end")
    
    
    def run_dump_restore_post_without_pk(self):
        print(f"pg_restore post (without PK) begin")
    
        with self.primary.execute_dump("post-data") as dump:
            self.secondary.execute_post_data_dump_without_pk(dump)
    
        print(f"run_dump_restore_post_without_pk end")
