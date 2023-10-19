# frozen_string_literal: true

require 'spec_helper'

if Que.db_version >= 8
  describe "Que Jobs Ext View" do
    class TestJob < Que::Job
      include Que::JobMethods

      def default_resolve_action
        # prevents default deletion of complete jobs for testing purposes
        finish
      end

      def run 
        sleep(0.1)
      end
    end

    class TestFailedJob < TestJob
      def run 
        raise Que::Error, 'Test Error'
      end
    end

    describe 'job.enqueue' do 
      it "should mirror enqueued job" do
        assert_equal 0, jobs_dataset.count
        assert_equal 0, jobs_ext_dataset.count

        TestJob.enqueue(
            1,
            'two',
            string: "string",
            integer: 5,
            array: [1, "two", {three: 3}],
            hash: {one: 1, two: 'two', three: [3]},
            job_options: { 
              priority: 4,
              queue: 'special_queue_name',
              run_at: Time.now
            }
          )

        assert_equal 1, jobs_dataset.count
        assert_equal 1, jobs_ext_dataset.count

        job = jobs_dataset.first
        ext_job = jobs_ext_dataset.first
        assert_equal ext_job[:queue], job[:queue]
        assert_equal ext_job[:priority], job[:priority]
        assert_equal ext_job[:run_at], job[:run_at]
        assert_equal ext_job[:first_run_at], job[:first_run_at]
        assert_equal ext_job[:job_class], job[:job_class]
        assert_equal ext_job[:args], job[:args]
        assert_equal ext_job[:job_schema_version], job[:job_schema_version]   

        jobs_dataset.delete

        assert_equal 0, jobs_dataset.count
        assert_equal 0, jobs_ext_dataset.count
      end

      it "should include additional lock data" do
        locker

        sleep_until_equal(1) { DB[:que_lockers].count }

        TestJob.enqueue

        
        assert_equal 1, jobs_dataset.count
        assert_equal 1, jobs_ext_dataset.count

        sleep_until { active_jobs_dataset.count.positive? }
        
        ext_job = jobs_ext_dataset.first

        assert_equal false, ext_job[:lock_id].nil?

        assert_equal ext_job[:lock_id], locked_ids.first
        assert_equal ext_job[:que_locker_pid], locked_pids.first

        locker.stop!
      end

      it "should add additional updated_at" do
        locker

        sleep_until_equal(1) { DB[:que_lockers].count }

        TestJob.enqueue

        ext_job = jobs_ext_dataset.first

        assert_equal ext_job[:run_at], ext_job[:updated_at]

        sleep_until { locked_ids.count.positive? }
        sleep_until { locked_ids.count.zero? }

        ext_job = jobs_ext_dataset.first

        assert_equal ext_job[:finished_at], ext_job[:updated_at]

        locker.stop!
      end

      describe "should include additional status" do
        let(:notified_errors) { [] }

        it "should set status to scheduled when run_at is in the future" do
          TestJob.enqueue(job_options: { run_at: Time.now + 1 })

          assert_equal jobs_ext_dataset.first[:status], 'scheduled'
        end

        it "should set status to queued when run_at is in the past and the job is not currently running, completed, failed or errored" do
          TestJob.enqueue(job_options: { run_at: Time.now - 1 })

          assert_equal jobs_ext_dataset.first[:status], 'queued'
        end

        it "should set status to running when the job has a lock associated with it" do
          locker
          
          sleep_until_equal(1) { DB[:que_lockers].count }
          
          TestJob.enqueue

          sleep_until { locked_ids.count.positive? }
          assert_equal jobs_ext_dataset.first[:status], 'running'

          locker.stop!
        end

        it "should set status to complete when finished_at is present" do
          locker
          
          sleep_until_equal(1) { DB[:que_lockers].count }
          
          TestJob.enqueue

          sleep_until { locked_ids.count.positive? }
          sleep_until { locked_ids.count.zero? }
          
          assert_equal jobs_ext_dataset.first[:status], 'completed'

          locker.stop!
        end

        it "should set status to errored when error_count is positive and expired_at is not present" do
          TestFailedJob.class_eval do 
            self.maximum_retry_count = 1
          end

          Que.error_notifier = proc { |e| notified_errors << e }

          locker
          
          sleep_until_equal(1) { DB[:que_lockers].count }

          TestFailedJob.enqueue

          sleep_until { locked_ids.count.positive? }
          sleep_until { locked_ids.count.zero? }

          assert_equal jobs_ext_dataset.first[:status], 'errored'
          assert_equal notified_errors.count, 1
          assert_equal notified_errors.first.message, 'Test Error'

          locker.stop!
        end

        it "should set status to failed when expired_at is present" do
          TestFailedJob.class_eval do 
            self.maximum_retry_count = 0
          end

          Que.error_notifier = proc { |e| notified_errors << e }

          locker
          
          sleep_until_equal(1) { DB[:que_lockers].count }

          TestFailedJob.enqueue

          sleep_until { locked_ids.count.positive? }
          sleep_until { locked_ids.count.zero? }
          sleep_until { expired_jobs_dataset.count.positive? }

          assert_equal jobs_ext_dataset.first[:status], 'failed'
          assert_equal notified_errors.count, 1
          assert_equal notified_errors.first.message, 'Test Error'

          locker.stop!
        end
      end
    end
  end
end