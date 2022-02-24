# frozen_string_literal: true

module Que
  VERSION = '1.2.0'

  def self.job_schema_version
    Gem::Version.new(Que::VERSION).segments.first
  end
end
